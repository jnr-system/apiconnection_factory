"""
楽楽販売から「待機中の注文」リストを取得し、楽天RMS APIを利用して対応する注文情報を検索・取得します。
RMSから取得した情報を楽楽販売の該当レコードにAPI経由で更新します。

【処理内容】
- 顧客情報・配送先情報の更新（郵便番号が空のレコード）
- 商品明細の追加（merchantDefinedSkuIdから9桁キーを抽出）
- ガスの種類をskuInfoから判定（都市ガス / プロパンガス / なし）
- 進捗をmerchantDefinedSkuIdから判定（工事込案件 / 自社在庫出荷 / 楽天倉庫出荷 / 楽天倉庫在庫）
- 決済完了ステータスの反映（RMSが発送待ち(300)かつ進捗に決済完了未設定のレコード）
"""
import base64
import requests
from datetime import datetime, timedelta
from pytz import timezone
import re
import sys
from pathlib import Path
import csv
import io
import time
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==============================================================================
# 設定
# ==============================================================================
RAKURAKU_DOMAIN    = "hntobias.rakurakuhanbai.jp"
RAKURAKU_TOKEN     = os.environ["RAKURAKU_TOKEN"]
RAKURAKU_SCHEMA_ID = "101357"  # データベースID
RAKURAKU_SEARCH_ID = "105786"  # 絞込み設定ID
RAKURAKU_LIST_ID   = "101263"  # レコード一覧画面設定ID

RMS_SERVICE_SECRET = os.environ["RMS_SERVICE_SECRET"]
RMS_LICENSE_KEY    = os.environ["RMS_LICENSE_KEY"]

LOG_FILE_PATH = Path(__file__).parent / "execution_log.txt"

# ==============================================================================
# ログ出力
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
# ガス種判定
# ==============================================================================
def parse_gas_type(sku_info):
    if not sku_info:
        return "なし"
    if "都市" in sku_info:
        return "都市ガス"
    if "LP" in sku_info or "プロパン" in sku_info:
        return "プロパンガス"
    return "なし"

# ==============================================================================
# 進捗判定
# ==============================================================================
def parse_progress(all_sku_ids):
    combined = " ".join(all_sku_ids)
    if "kouzi" in combined:
        return "工事込案件"
    if "zaiko" in combined:
        if "楽天" in combined:
            return "楽天倉庫出荷"
        return "自社在庫出荷"
    if "楽天倉庫在庫" in combined:
        return "楽天倉庫在庫"
    return ""

# ==============================================================================
# SKU → 商品キーリスト抽出
# ==============================================================================
def extract_product_keys(merchant_sku):
    return re.findall(r'\d{9}', merchant_sku or "")

# ==============================================================================
# 楽楽販売から待機データを取得
# ==============================================================================
def get_rakuraku_targets():
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN
    }
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

        csv_content = res.content.decode("utf-8-sig", errors="ignore")
        reader = csv.reader(io.StringIO(csv_content))

        target_map = {}

        try:
            next(reader)  # ヘッダー行をスキップ
        except StopIteration:
            write_log("楽楽販売: 対象データ(CSV)は0件でした。")
            return {}

        for row in reader:
            if len(row) < 39:
                continue

            key_id   = row[0].strip()
            rms_no   = row[1].strip()
            progress = row[4].strip()
            zip_code = row[38].strip()

            if not key_id or not rms_no:
                continue

            entry = {"keyId": key_id, "progress": progress}

            # 郵便番号が空 → 顧客情報更新対象（決済完了状態に関わらず必須）
            if not zip_code:
                entry["type"] = "customer"

            # 郵便番号あり・決済完了なし → 決済完了チェックのみ
            if zip_code and "決済完了" not in progress:
                entry["checkPayment"] = True

            if "type" not in entry and "checkPayment" not in entry:
                continue

            target_map[rms_no] = entry

        write_log(f"楽楽販売から {len(target_map)} 件の待機データを取得しました。")
        return target_map

    except Exception as e:
        write_log(f"楽楽販売(CSV取得)エラー: {e}")
        sys.exit(1)

# ==============================================================================
# RMSから注文情報を取得
# ==============================================================================
def get_rms_orders():
    token = base64.b64encode(f"{RMS_SERVICE_SECRET}:{RMS_LICENSE_KEY}".encode()).decode()
    headers = {
        "Authorization": f"ESA {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    jst = timezone("Asia/Tokyo")
    end_dt   = datetime.now(jst).strftime("%Y-%m-%dT%H:%M:%S+0900")
    start_dt = (datetime.now(jst) - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S+0900")

    search_url = "https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/"
    current_page = 1

    try:
        write_log(f"RMS検索開始: {start_dt} ～ {end_dt}")
        all_order_numbers = []

        while True:
            res = requests.post(
                search_url,
                headers=headers,
                json={
                    "dateType": 1,
                    "startDatetime": start_dt,
                    "endDatetime": end_dt,
                    "PaginationRequestModel": {
                        "requestRecordsAmount": 1000,
                        "requestPage": current_page,
                        "SortModelList": [{"sortColumn": 1, "sortDirection": 1}]
                    }
                },
                timeout=30
            )
            res.raise_for_status()

            current_list = res.json().get("orderNumberList", [])
            if not current_list:
                write_log(f"  - ページ {current_page} はデータなし。検索を終了します。")
                break

            all_order_numbers.extend(current_list)
            write_log(f"  - ページ {current_page} 取得成功: {len(current_list)}件 (累計: {len(all_order_numbers)}件)")

            if current_page >= 4:
                write_log("  - 設定された上限(4ページ)に達したため、これ以上の検索は行いません。")
                break

            current_page += 1

        if not all_order_numbers:
            write_log("RMS: 直近の注文はありませんでした。")
            return []

        # 詳細取得（100件ずつ）version=7: merchantDefinedSkuId・skuInfo取得に必要
        get_url = "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/"
        rms_details = []

        write_log(f"詳細データ取得中... (対象: {len(all_order_numbers)}件)")
        for i in range(0, len(all_order_numbers), 100):
            chunk = all_order_numbers[i:i + 100]
            sub_res = requests.post(get_url, headers=headers, json={"orderNumberList": chunk, "version": "7"}, timeout=30)
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
    payment_count = 0

    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"
    headers = {"Content-Type": "application/json", "X-HD-apitoken": RAKURAKU_TOKEN}

    for order in rms_orders:
        rms_number = order["orderNumber"]

        if rms_number not in rakuraku_map:
            continue

        entry  = rakuraku_map[rms_number]
        key_id = entry["keyId"]

        # ── 決済完了チェック ──
        if entry.get("checkPayment") and order.get("orderProgress") == 300:
            try:
                current_progress = [v.strip() for v in entry["progress"].split(",") if v.strip()]
                if "決済完了" not in current_progress:
                    current_progress.append("決済完了")
                res = requests.post(url, headers=headers, json={
                    "dbSchemaId": RAKURAKU_SCHEMA_ID,
                    "keyId": key_id,
                    "values": {"113100": current_progress}
                }, timeout=10)
                if res.status_code == 200:
                    write_log(f"  [決済完了] keyId={key_id} 進捗を「決済完了」に更新しました（既存: {entry['progress']}）")
                    payment_count += 1
                else:
                    write_log(f"  [決済完了 失敗] keyId={key_id}: {res.status_code} {res.text}")
                time.sleep(1.0)
            except Exception as e:
                write_log(f"  [決済完了 例外] keyId={key_id}: {e}")

        # ── 顧客情報更新 ──
        if entry.get("type") != "customer":
            continue

        update_count += 1

        orderer  = order["OrdererModel"]
        package  = order["PackageModelList"][0]
        sender   = package["SenderModel"]
        remarks  = order.get("remarks", "")

        match = re.search(r"(\d{4})-(\d{2})-(\d{2})", remarks)
        delivery_date = f"{match.group(1)}{match.group(2)}{match.group(3)}" if match else ""

        # 全SKU IDを収集
        all_sku_ids = []
        for item in package.get("ItemModelList", []):
            for sku in item.get("SkuModelList", []):
                sid = sku.get("merchantDefinedSkuId", "")
                if sid:
                    all_sku_ids.append(sid)

        # ガス種判定
        gas_type = "なし"
        for item in package.get("ItemModelList", []):
            for sku in item.get("SkuModelList", []):
                g = parse_gas_type(sku.get("skuInfo", ""))
                if g in ("都市ガス", "プロパンガス"):
                    gas_type = g
                    break
            if gas_type in ("都市ガス", "プロパンガス"):
                break

        # 進捗判定
        progress = parse_progress(all_sku_ids)

        # 明細行を構築
        detail_rows = []
        for item in package.get("ItemModelList", []):
            units = str(item.get("units", 1))
            for sku in item.get("SkuModelList", []):
                for key in extract_product_keys(sku.get("merchantDefinedSkuId", "")):
                    detail_rows.append({"113053": key, "113058": units})

        if not detail_rows:
            write_log(f"  [WARN] keyId={key_id}: 商品キーが抽出できませんでした。ヘッダのみ更新します。")

        # 1回目: ヘッダ更新
        try:
            res1 = requests.post(url, headers=headers, json={
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
                    "113089": f"{sender['phoneNumber1']}-{sender['phoneNumber2']}-{sender['phoneNumber3']}",
                    "113051": gas_type,
                    "113100": [progress] if progress else [],
                }
            }, timeout=10)
            if res1.status_code == 200:
                write_log(f"  [1/2 成功] keyId={key_id} ヘッダ更新完了")
            else:
                write_log(f"  [1/2 失敗] keyId={key_id} ヘッダ更新失敗: {res1.status_code} {res1.text}")
            time.sleep(1.0)
        except Exception as e:
            write_log(f"  [1/2 例外] keyId={key_id} ヘッダ更新中にエラー: {e}")

        # 2回目: 商品明細追加
        if detail_rows:
            try:
                res2 = requests.post(url, headers=headers, json={
                    "dbSchemaId": RAKURAKU_SCHEMA_ID,
                    "keyId": key_id,
                    "getSubordinate": "1",
                    "updateDetailKeyId": "detailKey",
                    "values": {"details": detail_rows}
                }, timeout=10)
                if res2.status_code == 200:
                    write_log(f"  [2/2 成功] keyId={key_id} 明細{len(detail_rows)}行追加完了（ガス種={gas_type}）")
                else:
                    write_log(f"  [2/2 失敗] keyId={key_id} 明細追加失敗: {res2.status_code} {res2.text}")
                time.sleep(1.0)
            except Exception as e:
                write_log(f"  [2/2 例外] keyId={key_id} 明細追加中にエラー: {e}")

    write_log(f"=== 処理完了: 顧客情報更新 {update_count} 件 / 決済完了更新 {payment_count} 件 ===")

if __name__ == "__main__":
    main()
