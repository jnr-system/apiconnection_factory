"""
楽楽販売のL業務管理DBから出荷日が今日以降かつ反映済みフラグが空のレコードを取得し、
RMS APIへ出荷日を登録後、楽楽販売の反映済みフラグに1を書き込む。
発送メール送信はPlaywrightによる別処理で行う。

【処理内容】
- L業務管理DBから対象レコードを取得（searchId: 107906）
- メーカー出荷日が今日以降のレコードのみを対象とする
- RMS APIで出荷確定（配送会社: その他、伝票番号: -）
- 処理成功後、楽楽販売の反映済みフラグ（116375）に1を書き込む
"""
import base64
import requests
from datetime import datetime
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
RAKURAKU_SEARCH_ID = "107906"

RMS_SERVICE_SECRET = os.environ["RMS_SERVICE_SECRET"]
RMS_LICENSE_KEY    = os.environ["RMS_LICENSE_KEY"]

LOG_FILE_PATH = Path(__file__).parent / "execution_log_tanpinday.txt"

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
# 楽楽販売からL業務管理DBの対象レコードを取得
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
        "listId": "101519",
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
            return []

        jst = timezone("Asia/Tokyo")
        today = datetime.now(jst).date()

        targets = []
        for row in reader:
            if len(row) < 5:
                continue

            key_id       = row[0].strip()
            rms_no       = row[1].strip()
            shipping_date_str = row[3].strip()

            if not key_id or not rms_no or not shipping_date_str:
                continue

            # 出荷日をパース（YYYY-MM-DD 形式を想定）
            try:
                shipping_date = datetime.strptime(shipping_date_str, "%Y-%m-%d").date()
            except ValueError:
                try:
                    shipping_date = datetime.strptime(shipping_date_str, "%Y/%m/%d").date()
                except ValueError:
                    write_log(f"  [WARN] keyId={key_id}: 出荷日フォーマット不明 ({shipping_date_str}), スキップします。")
                    continue

            # 今日以降のみ対象
            if shipping_date < today:
                continue

            progress = row[2].strip()

            targets.append({
                "keyId": key_id,
                "rmsNo": rms_no,
                "shippingDate": shipping_date_str,
                "sendMail": 0 if "工事" in progress else 1,
            })

        write_log(f"楽楽販売から {len(targets)} 件の対象レコードを取得しました。")
        return targets

    except Exception as e:
        write_log(f"楽楽販売(CSV取得)エラー: {e}")
        sys.exit(1)

# ==============================================================================
# RMSから注文詳細を取得してbasketIdを取得
# ==============================================================================
def get_shipping_info(rms_no, rms_headers):
    """basketId と shippingDetailId を取得する"""
    url = "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/"
    try:
        res = requests.post(url, headers=rms_headers, json={"orderNumberList": [rms_no], "version": "7"}, timeout=30)
        res.raise_for_status()
        order_list = res.json().get("OrderModelList", [])
        if not order_list:
            write_log(f"  [RMS] {rms_no}: 注文詳細が取得できませんでした。")
            return None, None
        order = order_list[0]
        pkg = order.get("PackageModelList", [{}])[0]
        basket_id = pkg.get("basketId")
        shipping_list = pkg.get("ShippingModelList", [])
        shipping_detail_id = shipping_list[0].get("shippingDetailId") if shipping_list else None
        return basket_id, shipping_detail_id
    except Exception as e:
        write_log(f"  [RMS 注文詳細取得 例外] {rms_no}: {e}")
        return None, None

# ==============================================================================
# RMSへ出荷確定
# ==============================================================================
def confirm_shipping_rms(rms_no, shipping_date_str, rms_headers):
    """
    RMS updateOrderShipping APIで出荷確定を行う。
    配送会社: その他（deliveryCompany=1000）、伝票番号: -
    """
    url = "https://api.rms.rakuten.co.jp/es/2.0/order/updateOrderShipping/"

    # 出荷日を YYYY-MM-DD 形式に変換
    shipping_date_fmt = shipping_date_str.replace("/", "-")

    basket_id, shipping_detail_id = get_shipping_info(rms_no, rms_headers)
    if basket_id is None or shipping_detail_id is None:
        write_log(f"  [RMS] {rms_no}: basketId/shippingDetailIdが取得できないためスキップします。")
        return False

    payload = {
        "orderNumber": rms_no,
        "BasketidModelList": [
            {
                "basketId": basket_id,
                "ShippingModelList": [
                    {
                        "shippingDetailId": shipping_detail_id,
                        "deliveryCompany": "1000",  # その他
                        "shippingNumber": "-",
                        "shippingDate": shipping_date_fmt,
                        "shippingDeleteFlag": 0
                    }
                ]
            }
        ]
    }

    try:
        res = requests.post(url, headers=rms_headers, json=payload, timeout=30)
        if not res.ok:
            write_log(f"  [RMS エラー詳細] {rms_no}: {res.status_code} {res.text}")
            return False
        result = res.json()

        # エラーチェック
        errors = result.get("MessageModelList", [])
        error_msgs = [m for m in errors if m.get("messageType") == "ERROR"]
        if error_msgs:
            write_log(f"  [RMS エラー] {rms_no}: {error_msgs}")
            return False

        write_log(f"  [RMS 成功] {rms_no}: 出荷確定完了（出荷日: {shipping_date_str}）")
        return True

    except Exception as e:
        write_log(f"  [RMS 例外] {rms_no}: {e}")
        return False

# ==============================================================================
# 楽楽販売の反映済みフラグを更新
# ==============================================================================
def update_rakuraku_flag(key_id, rr_headers):
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"
    try:
        res = requests.post(url, headers=rr_headers, json={
            "dbSchemaId": RAKURAKU_SCHEMA_ID,
            "keyId": key_id,
            "values": {"116375": "1"}
        }, timeout=10)
        if res.status_code == 200:
            write_log(f"  [楽楽 成功] keyId={key_id}: 反映済みフラグを1に更新しました。")
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
def main():
    write_log("=== 単品出荷日反映処理を開始します ===")

    targets = get_rakuraku_targets()
    if not targets:
        write_log("=== 処理対象なし。終了します。 ===")
        return

    # RMS認証ヘッダー
    token = base64.b64encode(f"{RMS_SERVICE_SECRET}:{RMS_LICENSE_KEY}".encode()).decode()
    rms_headers = {
        "Authorization": f"ESA {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    # 楽楽販売更新ヘッダー
    rr_headers = {
        "Content-Type": "application/json",
        "X-HD-apitoken": RAKURAKU_TOKEN
    }

    success_count = 0
    fail_count = 0

    for target in targets:
        key_id        = target["keyId"]
        rms_no        = target["rmsNo"]
        shipping_date = target["shippingDate"]
        send_mail     = target["sendMail"]  # 0=メールなし（工事込み）, 1=メールあり → Playwright側で使用

        write_log(f"処理中: keyId={key_id}, rmsNo={rms_no}, 出荷日={shipping_date}, メール={send_mail}")

        # RMSへ出荷確定
        rms_ok = confirm_shipping_rms(rms_no, shipping_date, rms_headers)
        time.sleep(1.0)

        if not rms_ok:
            fail_count += 1
            continue

        # 楽楽販売の反映済みフラグを更新
        rr_ok = update_rakuraku_flag(key_id, rr_headers)
        time.sleep(1.0)

        if rr_ok:
            success_count += 1
        else:
            fail_count += 1

    write_log(f"=== 処理完了: 成功 {success_count} 件 / 失敗 {fail_count} 件 ===")

if __name__ == "__main__":
    main()
