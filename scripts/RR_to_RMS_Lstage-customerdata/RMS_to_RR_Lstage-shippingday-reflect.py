"""
RMSの注文から出荷日を取得し、楽楽販売のL業務管理DBへ反映する。
楽楽販売側の進捗ステータスを「納期回答あり」に更新する。

【処理内容】
- 楽楽販売から「納期回答待ち」のレコードを取得（searchId: 105790）
- RMSから過去30日分の注文を取得
- 楽天注文番号で照合
- 一致したレコードの出荷日（113102）と進捗（113100: 納期回答あり）を更新
"""
import base64
import requests
from datetime import datetime, timedelta
from pytz import timezone
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
RAKURAKU_SCHEMA_ID = "101357"
RAKURAKU_SEARCH_ID = "105790"  # 納期回答待ち

RMS_SERVICE_SECRET = os.environ["RMS_SERVICE_SECRET"]
RMS_LICENSE_KEY    = os.environ["RMS_LICENSE_KEY"]

LOG_FILE_PATH = Path(__file__).parent / "execution_log_shippingday_reflect.txt"

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
# 楽楽販売から「納期回答待ち」のレコードを取得
# ==============================================================================
def get_rakuraku_targets():
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN
    }
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
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

        try:
            next(reader)  # ヘッダー行をスキップ
        except StopIteration:
            write_log("楽楽販売: 対象データ(CSV)は0件でした。")
            return {}

        target_map = {}
        for row in reader:
            if len(row) < 2:
                continue
            key_id = row[0].strip()
            rms_no = row[1].strip()
            if not key_id or not rms_no:
                continue
            target_map[rms_no] = {"keyId": key_id}

        write_log(f"楽楽販売から {len(target_map)} 件の納期回答待ちレコードを取得しました。")
        return target_map

    except Exception as e:
        write_log(f"楽楽販売(CSV取得)エラー: {e}")
        sys.exit(1)

# ==============================================================================
# RMSから注文を取得
# ==============================================================================
def get_rms_orders():
    token = base64.b64encode(f"{RMS_SERVICE_SECRET}:{RMS_LICENSE_KEY}".encode()).decode()
    headers = {
        "Authorization": f"ESA {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    jst = timezone("Asia/Tokyo")
    end_dt   = datetime.now(jst).strftime("%Y-%m-%dT%H:%M:%S+0900")
    start_dt = (datetime.now(jst) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S+0900")

    search_url = "https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/"
    current_page = 1
    all_order_numbers = []

    write_log(f"RMS検索開始: {start_dt} ～ {end_dt}")

    try:
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

            if current_page >= 10:
                write_log("  - 上限(10ページ)に達したため検索を終了します。")
                break

            current_page += 1

        if not all_order_numbers:
            write_log("RMS: 対象注文はありませんでした。")
            return {}

        get_url = "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/"
        order_map = {}

        write_log(f"詳細データ取得中... (対象: {len(all_order_numbers)}件)")
        for i in range(0, len(all_order_numbers), 100):
            chunk = all_order_numbers[i:i + 100]
            sub_res = requests.post(
                get_url,
                headers=headers,
                json={"orderNumberList": chunk, "version": "7"},
                timeout=30
            )
            sub_res.raise_for_status()
            for order in sub_res.json().get("OrderModelList", []):
                rms_no = order.get("orderNumber")
                if not rms_no:
                    continue
                shipping_date = None
                pkg_list = order.get("PackageModelList", [])
                if pkg_list:
                    shipping_list = pkg_list[0].get("ShippingModelList", [])
                    if shipping_list:
                        shipping_date = shipping_list[0].get("shippingDate")
                order_map[rms_no] = {"shippingDate": shipping_date}

        write_log(f"RMSから {len(order_map)} 件の詳細データを取得しました。")
        return order_map

    except Exception as e:
        write_log(f"RMS APIエラー: {e}")
        return {}

# ==============================================================================
# 楽楽販売のレコードを更新（出荷日 + 進捗）
# ==============================================================================
def update_rakuraku_record(key_id, shipping_date_str, rr_headers):
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"

    try:
        shipping_date_fmt = datetime.strptime(shipping_date_str, "%Y-%m-%d").strftime("%Y%m%d")
    except ValueError:
        shipping_date_fmt = shipping_date_str.replace("/", "").replace("-", "")

    try:
        res = requests.post(url, headers=rr_headers, json={
            "dbSchemaId": RAKURAKU_SCHEMA_ID,
            "keyId": key_id,
            "values": {
                "113102": shipping_date_fmt,
                "113100": ["納期回答あり"]
            }
        }, timeout=10)
        if res.status_code == 200:
            write_log(f"  [楽楽 成功] keyId={key_id}: 出荷日={shipping_date_fmt}, 進捗=納期回答あり")
            return True
        else:
            write_log(f"  [楽楽 失敗] keyId={key_id}: {res.status_code} {res.text}")
            return False
    except Exception as e:
        write_log(f"  [楽楽 例外] keyId={key_id}: {e}")
        return False

# ==============================================================================
# メイン処理
# ==============================================================================
def main(dry_run=False):
    mode_label = "[DRY-RUN] " if dry_run else ""
    write_log(f"=== {mode_label}RMS出荷日→楽楽販売反映処理を開始します ===")

    rakuraku_map = get_rakuraku_targets()
    if not rakuraku_map:
        write_log("=== 処理対象なし。終了します。 ===")
        return

    rms_order_map = get_rms_orders()
    if not rms_order_map:
        write_log("=== RMS注文なし。終了します。 ===")
        return

    rr_headers = {
        "Content-Type": "application/json",
        "X-HD-apitoken": RAKURAKU_TOKEN
    }

    success_count = 0
    fail_count = 0
    skip_count = 0

    for rms_no, rr_entry in rakuraku_map.items():
        if rms_no not in rms_order_map:
            skip_count += 1
            continue

        key_id        = rr_entry["keyId"]
        shipping_date = rms_order_map[rms_no]["shippingDate"]

        if not shipping_date:
            write_log(f"  [SKIP] rmsNo={rms_no}: RMSに出荷日が未登録のためスキップします。")
            skip_count += 1
            continue

        if dry_run:
            write_log(f"  [DRY-RUN] rmsNo={rms_no}, keyId={key_id}, 出荷日={shipping_date} → 113102={shipping_date}, 113100=[納期回答あり]")
            success_count += 1
            continue

        write_log(f"処理中: rmsNo={rms_no}, keyId={key_id}, 出荷日={shipping_date}")

        ok = update_rakuraku_record(key_id, shipping_date, rr_headers)
        if ok:
            success_count += 1
        else:
            fail_count += 1

        time.sleep(1.0)

    write_log(f"=== {mode_label}処理完了: 対象 {success_count} 件 / 失敗 {fail_count} 件 / スキップ {skip_count} 件 ===")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="楽楽販売への書き込みを行わず結果だけ表示する")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
