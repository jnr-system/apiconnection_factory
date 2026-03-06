"""
メール受信監視 → GCS アップロード → 楽楽販売 URL 書き込み (バックエンド用スクリプト)

IMAP で特定のメールボックスをポーリングし、未読メールを処理します。
※他の担当者が作成したフォーム（フロントエンド）から送信されるメールを監視します。

処理フロー:
  1. IMAP で未読メールを取得
  2. 【仮】件名や本文から「顧客名」「電話番号」を抽出
  3. 添付画像を Pillow で圧縮し、GCS にアップロード
  4. 楽楽販売のレコード作成を待つため、別スレッドで5分待機してURLを書き込み
  5. 処理が開始されたメールは既読にマーク
"""

import argparse
import csv
import email
import email.header
import imaplib
import io
import json
import os
import re
import sys
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests
from google.cloud import storage
from google.oauth2 import service_account
from PIL import Image, ImageOps

LOG_FILE_PATH = Path(__file__).parent / "execution_log.txt"

def write_log(msg):
    try:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{now_str}] {msg}"
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")
        import builtins
        builtins.print(log_msg)
    except Exception:
        pass

# ──────────────────────────────────────────────
# 設定（環境変数から読み込み）
# ──────────────────────────────────────────────

# IMAP 設定
IMAP_HOST     = os.environ.get("IMAP_HOST", "sv1227.xserver.jp")
IMAP_PORT     = int(os.environ.get("IMAP_PORT", "993"))
EMAIL_ADDRESS = os.environ.get("EMAIL_ADDRESS", "customer9@syouzikiya.jp")
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

# 対象とするメールの件名（これ以外のメールは無視する）
TARGET_SUBJECT = "【写真受付】" 

# GCS 設定
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "rakuraku-photos_factory-jnr")
GCS_PREFIX = "photos"

# 楽楽販売 設定
RAKURAKU_DOMAIN  = os.environ.get("RAKURAKU_DOMAIN", "hntobias.rakurakuhanbai.jp")
RAKURAKU_ACCOUNT = os.environ.get("RAKURAKU_ACCOUNT", "mspy4wa")
RAKURAKU_TOKEN   = os.environ["RAKURAKU_TOKEN"]

RAKURAKU_INQUIRY_DB_ID     = os.environ.get("RAKURAKU_INQUIRY_DB_ID", "101443")
RAKURAKU_INQUIRY_SEARCH_ID = os.environ.get("RAKURAKU_INQUIRY_SEARCH_ID", "107323")
RAKURAKU_INQUIRY_LIST_ID   = os.environ.get("RAKURAKU_INQUIRY_LIST_ID", "101471")
RAKURAKU_URL_FIELD_ID      = os.environ.get("RAKURAKU_URL_FIELD_ID", "115927")

# サーバーベースURL（楽楽販売に書き込む閲覧用URL）
SERVER_BASE_URL = os.environ.get("SERVER_BASE_URL", "http://127.0.0.1:8000")


# ──────────────────────────────────────────────
# GCS クライアント
# ──────────────────────────────────────────────

def _get_gcs_credentials() -> service_account.Credentials:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        secret_path = Path(__file__).parent / "google_secret.json"
        if secret_path.exists():
            raw = secret_path.read_text(encoding="utf-8")
        else:
            raise RuntimeError("GCS認証情報が見つかりません。")
    creds_dict = json.loads(raw)
    return service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

def get_storage_client() -> storage.Client:
    credentials = _get_gcs_credentials()
    return storage.Client(credentials=credentials, project=credentials.project_id)

def get_bucket() -> storage.Bucket:
    return get_storage_client().bucket(BUCKET_NAME)


# ──────────────────────────────────────────────
# 楽楽販売 連携処理 (遅延実行用)
# ──────────────────────────────────────────────

def _rakuraku_headers() -> dict:
    return {"Content-Type": "application/json", "X-HD-apitoken": RAKURAKU_TOKEN}

def find_rakuraku_record_id(phone: str) -> Optional[dict]:
    url = f"https://{RAKURAKU_DOMAIN}/{RAKURAKU_ACCOUNT}/api/csvexport/version/v1"
    payload = {
        "dbSchemaId": RAKURAKU_INQUIRY_DB_ID,
        "listId":     RAKURAKU_INQUIRY_LIST_ID,
        "searchId":   RAKURAKU_INQUIRY_SEARCH_ID,
        "limit":      500,
    }
    try:
        res = requests.post(url, headers=_rakuraku_headers(), json=payload, timeout=30)
        res.raise_for_status()
    except Exception as e:
        print(f"[ERROR] 楽楽販売 CSV取得エラー: {e}")
        return None

    # UTF-8 でデコード（BOM付きにも対応できるよう utf-8-sig を使用）
    csv_text = res.content.decode("utf-8-sig", errors="ignore")
    rows = list(csv.reader(io.StringIO(csv_text)))
    if len(rows) < 2: 
        print(f"  [DEBUG] CSVデータが空、またはヘッダーしかありません。（行数: {len(rows)}）")
        return None

    header = rows[0]
    print(f"  [DEBUG] 楽楽販売CSVの列名 (全{len(header)}列): {', '.join(header)}")

    idx_id   = next((i for i, h in enumerate(header) if "ID" in h), -1)
    idx_tel  = next((i for i, h in enumerate(header) if "電話番号" in h), -1)
    
    print(f"  [DEBUG] → 'ID' を含む列番号: {idx_id}, '電話番号' を含む列番号: {idx_tel}")

    if idx_id == -1: 
        print("  [DEBUG] エラー: IDが含まれる列が見つかりません。")
        return None
    if idx_tel == -1:
        print("  [DEBUG] エラー: 電話番号が含まれる列が見つかりません。")
        return None

    phone_norm = phone.replace("-", "").replace("ー", "").replace("−", "")
    print(f"  [DEBUG] 探している電話番号（ハイフンなし）: {phone_norm}")

    for r_idx, row in enumerate(rows[1:], start=1):
        tel = row[idx_tel].replace("-", "") if idx_tel != -1 and len(row) > idx_tel else ""
        if phone_norm == tel:
            print(f"  [DEBUG] ✔️ {r_idx}行目で一致しました！ (登録内容: {row})")
            return {"key_id": row[idx_id].strip()}
        
    print(f"  [DEBUG] ❌ 探しましたが、全{len(rows)-1}件のレコードの中に一致する番号はありませんでした。")
    return None

def update_rakuraku_photo_url(key_id: str, view_url: str) -> dict:
    url = f"https://{RAKURAKU_DOMAIN}/{RAKURAKU_ACCOUNT}/apirecord/update/version/v1"
    payload = {
        "dbSchemaId": RAKURAKU_INQUIRY_DB_ID,
        "keyId":      key_id,
        "values":     {RAKURAKU_URL_FIELD_ID: view_url},
    }
    try:
        res = requests.post(url, headers=_rakuraku_headers(), json=payload, timeout=30)
        res.raise_for_status()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ★ここに遅延実行スレッド用の関数を定義します
def delayed_rakuraku_update(phone: str, customer_name: str, yyyymm: str):
    """
    別スレッドで実行され、メール監視を止めずに5分待機して楽楽販売を更新する
    """
    INITIAL_DELAY = int(os.environ.get("RAKURAKU_DELAY_SECONDS", "30"))
    RETRY_INTERVAL = 120
    MAX_RETRIES = 3

    print(f"  [Thread] ⏳ {customer_name} 様の楽楽販売連携: {INITIAL_DELAY}秒待機します...")
    time.sleep(INITIAL_DELAY)

    phone_clean = phone.replace("-", "").replace("ー", "").replace("−", "")
    # yyyymm/顧客名 をエンコード（スラッシュはそのまま）
    encoded_folder = quote(f"{yyyymm}/{customer_name}", safe="/")
    view_url = f"{SERVER_BASE_URL}/view/{encoded_folder}"

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"  [Thread] 🔄 {customer_name} 様のレコードを検索 (試行 {attempt}/{MAX_RETRIES})...")
        rakuraku_info = find_rakuraku_record_id(phone_clean)

        if rakuraku_info:
            res = update_rakuraku_photo_url(rakuraku_info["key_id"], view_url)
            if res.get("success"):
                print(f"  [Thread] ✅ 楽楽販売更新完了: {customer_name} (ID: {rakuraku_info['key_id']})")
            else:
                print(f"  [Thread] ❌ 楽楽販売更新失敗: {customer_name}")
            break
        else:
            if attempt < MAX_RETRIES:
                print(f"  [Thread] ⚠️ {customer_name} 様のレコード未発見。{RETRY_INTERVAL}秒後にリトライ...")
                time.sleep(RETRY_INTERVAL)
            else:
                print(f"  [Thread] ❌ {customer_name} 様のレコードが見つからずタイムアウトしました。")


# ──────────────────────────────────────────────
# メール処理ロジック
# ──────────────────────────────────────────────

def decode_mime_text(raw_text: str) -> str:
    parts = []
    for data, charset in email.header.decode_header(raw_text):
        if isinstance(data, bytes):
            parts.append(data.decode(charset or "utf-8", errors="replace"))
        else:
            parts.append(data)
    return "".join(parts)

def extract_body_text(msg) -> str:
    """メール本文を抽出する"""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get("Content-Disposition"))
            if ctype == "text/plain" and "attachment" not in cdispo:
                try:
                    body += part.get_payload(decode=True).decode("utf-8", "ignore")
                except:
                    pass
    else:
        try:
            body = msg.get_payload(decode=True).decode("utf-8", "ignore")
        except:
            pass
    return body

def parse_customer_info(subject: str, body: str) -> dict:
    """
    メール本文から顧客名と電話番号を抽出します。
    フォーマット例:
    お名前: JNRシステム部
    電話番号: 0120555555
    """
    name = "不明なお客様"
    phone = "000-0000-0000"

    # 「お名前:」または「お名前：」から改行までの文字を取得
    match_name = re.search(r"お名前[:：]\s*(.+)", body)
    if match_name:
        name = match_name.group(1).strip()

    # 「電話番号:」または「電話番号：」から連続する数字（とハイフン）を取得
    match_phone = re.search(r"電話番号[:：]\s*([\d\-]+)", body)
    if match_phone:
        phone = match_phone.group(1).strip()

    return {"name": name, "phone": phone}


def process_emails():
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] メールチェック開始...")
    try:
        imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        imap.select("INBOX")

        status, data = imap.search(None, "UNSEEN")
        if status != "OK" or not data[0]:
            return

        mail_ids = data[0].split()
        print(f"  未読メール: {len(mail_ids)} 件")

        for mail_id in mail_ids:
            _process_single_email(imap, mail_id)

    except Exception as e:
        print(f"[ERROR] 通信エラー: {e}")
    finally:
        try:
            imap.close()
            imap.logout()
        except:
            pass


def _process_single_email(imap, mail_id):
    # BODY.PEEK[] を使うことで、メールを取得しても自動で「既読」になりません
    status, msg_data = imap.fetch(mail_id, "(BODY.PEEK[])")
    if status != "OK": return

    msg = email.message_from_bytes(msg_data[0][1])
    subject = decode_mime_text(msg.get("Subject", ""))
    
    # 1. 件名でフィルタリング（関係ないメールは無視して既読にしない）
    if TARGET_SUBJECT not in subject:
        # ログも出さずにスルーします（他の人が使うメールボックスを邪魔しない）
        return
    
    print(f"\n  📥 処理対象メールを受信: {subject}")

    # 2. 顧客情報の抽出と年月フォルダの決定
    body = extract_body_text(msg)
    info = parse_customer_info(subject, body)
    customer_name = info["name"]
    customer_phone = info["phone"]
    yyyymm = datetime.now().strftime("%Y%m")

    # 3. 添付ファイルの抽出と圧縮・アップロード
    bucket = get_bucket()
    uploaded_count = 0

    MAX_DIMENSION = 1920
    JPEG_QUALITY = 85

    for part in msg.walk():
        if "attachment" not in str(part.get("Content-Disposition", "")):
            continue

        filename = decode_mime_text(part.get_filename() or f"attachment_{uploaded_count}.jpg")
        content = part.get_payload(decode=True)
        if not content: continue
        
        original_size = len(content)
        content_type = part.get_content_type()

        # 画像の圧縮処理
        try:
            img = Image.open(io.BytesIO(content))
            try: img = ImageOps.exif_transpose(img)
            except: pass

            w, h = img.size
            if max(w, h) > MAX_DIMENSION:
                ratio = MAX_DIMENSION / max(w, h)
                img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)

            buf = io.BytesIO()
            if img.mode in ("RGBA", "P"): img = img.convert("RGB")
            img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
            
            content = buf.getvalue()
            content_type = "image/jpeg"
            filename = f"{os.path.splitext(filename)[0]}.jpg"
            
            comp_size = len(content)
            pct = (1 - comp_size / original_size) * 100
            print(f"    📦 圧縮: {filename} ({pct:.0f}% サイズ削減)")
        except Exception:
            pass # 画像以外はそのまま

        # GCSへアップロード (年月フォルダを追加)
        blob_name = f"{GCS_PREFIX}/{yyyymm}/{customer_name}/{filename}"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type=content_type or "application/octet-stream")
        uploaded_count += 1
        print(f"    ✅ アップロード完了: {blob_name}")

    # 4. 既読にして、別スレッドで楽楽販売の遅延更新を開始
    imap.store(mail_id, "+FLAGS", "\\Seen")
    
    if uploaded_count > 0:
        threading.Thread(
            target=delayed_rakuraku_update,
            args=(customer_phone, customer_name, yyyymm),
            daemon=True
        ).start()
    else:
        print("    ⚠️ 添付ファイルがありませんでした")


# ──────────────────────────────────────────────
# 起動
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()

    print("=" * 50)
    print(" バックエンド専用: メール監視＆画像アップロード")
    print(f" 監視メール: {EMAIL_ADDRESS} (対象件名: {TARGET_SUBJECT})")
    print("=" * 50)

    if args.daemon:
        while True:
            process_emails()
            time.sleep(args.interval)
    else:
        process_emails()
