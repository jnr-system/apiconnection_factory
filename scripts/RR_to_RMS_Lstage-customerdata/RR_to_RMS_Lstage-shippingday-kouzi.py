"""
楽楽販売のL業務管理DBから「工事手配完了」のレコードを取得し、
メーカー出荷日をRMSへ出荷確定として反映する。

【処理内容】
- 楽楽販売から「工事手配完了」のレコードを取得（searchId: 105858）
- RMSへ注文番号リストをまとめて投げ、出荷日が未登録のものだけを対象とする
- RMS APIで出荷確定（配送会社: その他、伝票番号: -）
"""
import base64
import requests
from datetime import datetime
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
RAKURAKU_SEARCH_ID = "105858"  # 工事手配完了

RMS_SERVICE_SECRET = os.environ["RMS_SERVICE_SECRET"]
RMS_LICENSE_KEY    = os.environ["RMS_LICENSE_KEY"]

LOG_FILE_PATH = Path(__file__).parent / "execution_log_kouzi.txt"

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
# 楽楽販売から「工事手配完了」のレコードを取得
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
        "limit": 100
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=30)
        if res.status_code != 200:
            write_log(f"楽楽販売エラー: {res.status_code} {res.text}")
            sys.exit(1)

        csv_content = res.content.decode("utf-8-sig", errors="ignore")
        reader = csv.reader(io.StringIO(csv_content))

        try:
            api_headers = next(reader)
        except StopIteration:
            write_log("楽楽販売: 対象データ(CSV)は0件でした。")
            return {}

        try:
            idx_key_id       = api_headers.index("注文ID")
            idx_rms_no       = api_headers.index("楽天受注番号")
            idx_shipping_date = api_headers.index("メーカー出荷日")
        except ValueError as e:
            write_log(f"楽楽販売CSVにヘッダーが見つかりません: {e}")
            sys.exit(1)

        target_map = {}
        for row in reader:
            if len(row) <= max(idx_key_id, idx_rms_no, idx_shipping_date):
                continue
            key_id            = row[idx_key_id].strip()
            rms_no            = row[idx_rms_no].strip()
            shipping_date_str = row[idx_shipping_date].strip()

            if not key_id or not rms_no or not shipping_date_str:
                continue

            try:
                shipping_date = datetime.strptime(shipping_date_str, "%Y-%m-%d").date()
            except ValueError:
                try:
                    shipping_date = datetime.strptime(shipping_date_str, "%Y/%m/%d").date()
                except ValueError:
                    write_log(f"  [WARN] keyId={key_id}: 出荷日フォーマット不明 ({shipping_date_str}), スキップします。")
                    continue

            target_map[rms_no] = {
                "keyId": key_id,
                "shippingDate": shipping_date.strftime("%Y-%m-%d"),
            }

        write_log(f"楽楽販売から {len(target_map)} 件の工事手配完了レコードを取得しました。")
        return target_map

    except Exception as e:
        write_log(f"楽楽販売(CSV取得)エラー: {e}")
        sys.exit(1)

# ==============================================================================
# RMSから注文詳細を一括取得し、出荷日未登録のものだけ返す
# ==============================================================================
def get_rms_unshipped(order_numbers, rms_headers):
    """
    注文番号リストをまとめてgetOrderに投げ、
    出荷日(shippingDate)が未登録の注文のみ basketId・shippingDetailId とともに返す。
    """
    url = "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/"
    result = {}

    try:
        res = requests.post(
            url,
            headers=rms_headers,
            json={"orderNumberList": order_numbers, "version": "7"},
            timeout=30
        )
        res.raise_for_status()

        for order in res.json().get("OrderModelList", []):
            rms_no = order.get("orderNumber")
            if not rms_no:
                continue
            pkg = order.get("PackageModelList", [{}])[0]
            basket_id      = pkg.get("basketId")
            shipping_list  = pkg.get("ShippingModelList", [])
            shipping_detail_id = shipping_list[0].get("shippingDetailId") if shipping_list else None
            existing_date      = shipping_list[0].get("shippingDate") if shipping_list else None

            if existing_date:
                continue

            if not basket_id:
                write_log(f"  [WARN] {rms_no}: basketIdが取得できません。スキップします。")
                continue

            result[rms_no] = {
                "basketId": basket_id,
                "shippingDetailId": shipping_detail_id,  # Noneの場合は新規登録
            }

    except Exception as e:
        write_log(f"RMS getOrder エラー: {e}")

    return result

# ==============================================================================
# RMSへ出荷確定
# ==============================================================================
def confirm_shipping_rms(rms_no, shipping_date_str, basket_id, shipping_detail_id, rms_headers):
    url = "https://api.rms.rakuten.co.jp/es/2.0/order/updateOrderShipping/"
    shipping_entry = {
        "deliveryCompany": "1000",  # その他
        "shippingNumber": "-",
        "shippingDate": shipping_date_str,
        "shippingDeleteFlag": 0
    }
    if shipping_detail_id is not None:
        shipping_entry["shippingDetailId"] = shipping_detail_id

    payload = {
        "orderNumber": rms_no,
        "BasketidModelList": [
            {
                "basketId": basket_id,
                "ShippingModelList": [shipping_entry]
            }
        ]
    }

    try:
        res = requests.post(url, headers=rms_headers, json=payload, timeout=30)
        if not res.ok:
            write_log(f"  [RMS エラー詳細] {rms_no}: {res.status_code} {res.text}")
            return False
        errors = [m for m in res.json().get("MessageModelList", []) if m.get("messageType") == "ERROR"]
        if errors:
            write_log(f"  [RMS エラー] {rms_no}: {errors}")
            return False
        write_log(f"  [RMS 成功] {rms_no}: 出荷確定完了（出荷日: {shipping_date_str}）")
        return True
    except Exception as e:
        write_log(f"  [RMS 例外] {rms_no}: {e}")
        return False

# ==============================================================================
# メイン処理
# ==============================================================================
def main(dry_run=False):
    mode_label = "[DRY-RUN] " if dry_run else ""
    write_log(f"=== {mode_label}工事手配完了 出荷日反映処理を開始します ===")

    rakuraku_map = get_rakuraku_targets()
    if not rakuraku_map:
        write_log("=== 処理対象なし。終了します。 ===")
        return

    token = base64.b64encode(f"{RMS_SERVICE_SECRET}:{RMS_LICENSE_KEY}".encode()).decode()
    rms_headers = {
        "Authorization": f"ESA {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    # RMSへ一括照会して出荷日未登録のものだけ取得
    rms_unshipped = get_rms_unshipped(list(rakuraku_map.keys()), rms_headers)
    write_log(f"RMS照会結果: {len(rms_unshipped)} 件が出荷日未登録（対象）")

    if not rms_unshipped:
        write_log("=== 出荷確定対象なし。終了します。 ===")
        return

    success_count = 0
    fail_count = 0

    for rms_no, rms_info in rms_unshipped.items():
        if rms_no not in rakuraku_map:
            continue

        key_id        = rakuraku_map[rms_no]["keyId"]
        shipping_date = rakuraku_map[rms_no]["shippingDate"]
        basket_id     = rms_info["basketId"]
        shipping_detail_id = rms_info["shippingDetailId"]

        if dry_run:
            write_log(f"  [DRY-RUN] rmsNo={rms_no}, keyId={key_id}, 出荷日={shipping_date} → RMS出荷確定（その他/-）")
            success_count += 1
            continue

        write_log(f"処理中: rmsNo={rms_no}, keyId={key_id}, 出荷日={shipping_date}")

        ok = confirm_shipping_rms(rms_no, shipping_date, basket_id, shipping_detail_id, rms_headers)
        if ok:
            success_count += 1
        else:
            fail_count += 1

        time.sleep(1.0)

    write_log(f"=== {mode_label}処理完了: 成功 {success_count} 件 / 失敗 {fail_count} 件 ===")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="RMSへの書き込みを行わず結果だけ表示する")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
