import base64
import requests
from datetime import datetime, timedelta
from pytz import timezone
import re
import sys
from pathlib import Path
import csv
import io
import time  # ★追加: 休憩用モジュール

# ==============================================================================
# 設定エリア
# ==============================================================================

# 楽楽販売の設定
RAKURAKU_DOMAIN = "hntobias.rakurakuhanbai.jp"
RAKURAKU_TOKEN = "c2lu3A6RCMM6PmizVcNqQ9Rt6uzl0ouiAU1yTYtfkxJnN5EmE1iAfJcTLidl8BzG"

# 以前教えてもらったID情報
RAKURAKU_SCHEMA_ID = "101357"  # データベースID
RAKURAKU_SEARCH_ID = "105786"  # 絞込み設定ID
RAKURAKU_LIST_ID   = "101263"  # レコード一覧画面設定ID

# RMS API設定
RMS_SERVICE_SECRET = "SP430009_FkH4HrpLeoYsLFJz"
RMS_LICENSE_KEY = "SL430009_H1ntjtEZdZ41P1zA"

# ログファイルパス
LOG_FILE_PATH = Path(__file__).parent / "execution_log.txt"

# ==============================================================================
# 共通関数: ログ出力
# ==============================================================================
def write_log(message):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{now_str}] {message}"
    print(log_msg)
    try:
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")
    except Exception as e:
        print(f"ログ書き込みエラー: {e}")

# ==============================================================================
# 1. 楽楽販売から「待機中の注文」をCSV経由で取得
# ==============================================================================
def get_rakuraku_targets():
    
    # API URL (csvexport)
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
    
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN
    }

    # limit: 1000 を指定して大量取得に対応
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "listId": RAKURAKU_LIST_ID,    
        "searchId": RAKURAKU_SEARCH_ID,
        "limit": 1000  
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if res.status_code != 200:
            write_log(f"楽楽販売エラー: {res.status_code} {res.text}")
            sys.exit(1)
            
        csv_content = res.content.decode("cp932", errors="ignore")
        f_obj = io.StringIO(csv_content)
        reader = csv.reader(f_obj)
        
        target_map = {}
        
        # データがあるか確認
        try:
            # 1行目(ヘッダー)を読み飛ばす
            header = next(reader) 
        except StopIteration:
            write_log("楽楽販売: 対象データ(CSV)は0件でした。")
            return {}

        for row in reader:
            if len(row) < 2:
                continue
            
            key_id = row[0].strip()
            rms_no = row[1].strip()
            
            if key_id and rms_no:
                target_map[rms_no] = key_id

        write_log(f"楽楽販売から {len(target_map)} 件の待機データを取得しました。")
        return target_map

    except Exception as e:
        write_log(f"楽楽販売(CSV取得)エラー: {e}")
        sys.exit(1)

# ==============================================================================
# 2. RMSから注文情報を取得
# ==============================================================================
def get_rms_orders():
    token = base64.b64encode(f"{RMS_SERVICE_SECRET}:{RMS_LICENSE_KEY}".encode()).decode()
    headers = {
        "Authorization": f"ESA {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    jst = timezone("Asia/Tokyo")
    end_dt = datetime.now(jst).strftime("%Y-%m-%dT%H:%M:%S+0900")
    
    # 期間を「10日前」まで拡大
    start_dt = (datetime.now(jst) - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S+0900")

    search_url = "https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/"
    
    current_page = 1
    
    search_payload = {
        "dateType": 1,
        "startDatetime": start_dt,
        "endDatetime": end_dt,
        "PaginationRequestModel": {
            "requestRecordsAmount": 1000,
            "requestPage": current_page,
            "SortModelList": [{"sortColumn": 1, "sortDirection": 1}]
        }
    }

    try:
        write_log(f"RMS検索開始: {start_dt} ～ {end_dt}")
        all_order_numbers = []
        
        # --- 強制ループ処理開始 (最大4ページで停止) ---
        while True:
            search_payload["PaginationRequestModel"]["requestPage"] = current_page
            
            res = requests.post(search_url, headers=headers, json=search_payload, timeout=30)
            res.raise_for_status()
            
            data = res.json()
            current_list = data.get("orderNumberList", [])
            
            if not current_list:
                write_log(f"  - ページ {current_page} はデータなし。検索を終了します。")
                break
            
            count_in_page = len(current_list)
            all_order_numbers.extend(current_list)
            
            write_log(f"  - ページ {current_page} 取得成功: {count_in_page}件 (累計: {len(all_order_numbers)}件)")
            
            if current_page >= 4:
                write_log("  - 設定された上限(4ページ)に達したため、これ以上の検索は行いません。")
                break

            current_page += 1
        # --- ループ処理終了 ---

        if not all_order_numbers:
            write_log("RMS: 直近の注文はありませんでした。")
            return []

        # 詳細情報の取得処理 (100件ずつ)
        get_url = "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/"
        rms_details = []
        chunk_size = 100
        
        write_log(f"詳細データ取得中... (対象: {len(all_order_numbers)}件)")
        
        for i in range(0, len(all_order_numbers), chunk_size):
            chunk = all_order_numbers[i:i + chunk_size]
            payload = {"orderNumberList": chunk, "version": "6"}
            
            sub_res = requests.post(get_url, headers=headers, json=payload, timeout=30)
            sub_res.raise_for_status()
            rms_details.extend(sub_res.json().get("OrderModelList", []))

        write_log(f"RMSから {len(rms_details)} 件の詳細データを取得しました。")
        return rms_details

    except Exception as e:
        write_log(f"RMS APIエラー: {e}")
        import traceback
        traceback.print_exc()
        return []

# ==============================================================================
# メイン処理
# ==============================================================================
def main():
    write_log("=== 自動連携処理を開始します ===")

    rakuraku_map = get_rakuraku_targets()
    if not rakuraku_map:
        return

    rms_orders = get_rms_orders()
    if not rms_orders:
        return

    update_count = 0
    
    for order in rms_orders:
        rms_number = order["orderNumber"]
        
        if rms_number in rakuraku_map:
            key_id = rakuraku_map[rms_number]
            
            orderer = order["OrdererModel"]
            package = order["PackageModelList"][0]
            sender = package["SenderModel"]
            remarks = order.get("remarks", "")
            
            match = re.search(r"(\d{4})-(\d{2})-(\d{2})", remarks)
            delivery_date = f"{match.group(1)}{match.group(2)}{match.group(3)}" if match else ""

            update_payload = {
                "dbSchemaId": RAKURAKU_SCHEMA_ID,
                "keyId": key_id,
                "values": {
                    "113811": f"{orderer['phoneNumber1']}-{orderer['phoneNumber2']}-{orderer['phoneNumber3']}",
                    "113772": str(order["totalPrice"]),
                    "113095": f"{sender['zipCode1']}{sender['zipCode2']}",
                    "113073": delivery_date,
                    "113096": sender['prefecture'],
                    "113097": sender['city'],
                    "113098": sender['subAddress'],
                    "113089": f"{sender['phoneNumber1']}-{sender['phoneNumber2']}-{sender['phoneNumber3']}"
                }
            }
            
            try:
                # 更新API
                url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"
                headers = {"Content-Type": "application/json", "X-HD-apitoken": RAKURAKU_TOKEN}
                
                upd_res = requests.post(url, headers=headers, json=update_payload, timeout=10)
                
                if upd_res.status_code == 200:
                    write_log(f"[成功] RMS:{rms_number} -> 楽楽ID:{key_id} を更新しました。")
                    update_count += 1
                    # ★ここが重要！連続アクセスを避けるために1秒休憩する
                    time.sleep(1.0)
                else:
                    write_log(f"[失敗] ID:{key_id} の更新失敗: {upd_res.status_code} {upd_res.text}")
                    # 失敗した場合も念のため少し待つ
                    time.sleep(1.0)
                    
            except Exception as e:
                write_log(f"[例外] ID:{key_id} 更新中にエラー: {e}")

    write_log(f"=== 処理完了: 合計 {update_count} 件を更新しました ===")

if __name__ == "__main__":
    main()