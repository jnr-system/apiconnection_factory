"""
Zoom Revenue Accelerator (ZRA) の会議要約・次のステップを
楽楽販売の対応履歴フィールドへ自動反映するスクリプト。

【処理フロー】
1. ZRA Webhook受信 or ポーリングで会議データ取得
2. GET /iq/conversations/{id} → 要約・次のステップ取得
3. GET /zra/conversations/{id}/interactions → 参加者の電話番号取得
4. 楽楽販売CSVエクスポートAPIで電話番号マッチング → keyId特定
5. 楽楽販売レコード更新APIで対応履歴フィールドへ書き込み

【注意事項（要確認）】
- RAKURAKU_SCHEMA_ID / HISTORY_FIELD_ID は自環境の値に要変更
- 楽楽販売の対応履歴フィールドIDは管理画面のAPI設定から確認すること
- 電話番号フィールドIDも同様に確認が必要
- ZRA APIはrevenue_accelerator:read:admin スコープが必要
- ZRA側の要約・次のステップは /iq/conversations/{id} に含まれる想定
  （実際のレスポンス構造は要検証）
"""

import os
import re
import csv
import io
import time
import json
import requests
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==============================================================================
# 設定
# ==============================================================================

# --- Zoom ZRA ---
ZOOM_ACCOUNT_ID    = os.environ["ZOOM_ACCOUNT_ID"]
ZOOM_CLIENT_ID     = os.environ["ZOOM_CLIENT_ID"]
ZOOM_CLIENT_SECRET = os.environ["ZOOM_CLIENT_SECRET"]

# --- 楽楽販売 ---
RAKURAKU_DOMAIN    = os.environ.get("RAKURAKU_DOMAIN", "hntobias.rakurakuhanbai.jp")
RAKURAKU_TOKEN     = os.environ["RAKURAKU_TOKEN"]
RAKURAKU_SCHEMA_ID = "XXXXXX"   # ★ 要変更: 楽楽販売のデータベースID
RAKURAKU_LIST_ID   = "XXXXXX"   # ★ 要変更: レコード一覧画面設定ID

# 楽楽販売フィールドID（管理画面のAPI設定から確認）
FIELD_PHONE        = "XXXXXX"   # ★ 要変更: 電話番号フィールドID
FIELD_HISTORY      = "XXXXXX"   # ★ 要変更: 対応履歴フィールドID

# ポーリングモード時の検索期間（時間）
POLL_HOURS = 1

LOG_FILE_PATH = Path(__file__).parent / "zoom_to_rakuraku.log"

# ==============================================================================
# ログ設定
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)

# ==============================================================================
# Zoom認証（Server-to-Server OAuth）
# ==============================================================================
_zoom_token_cache: dict = {}

def get_zoom_access_token() -> str:
    """アクセストークンを取得（1時間キャッシュ）"""
    now = time.time()
    if _zoom_token_cache.get("expires_at", 0) > now + 60:
        return _zoom_token_cache["token"]

    res = requests.post(
        "https://zoom.us/oauth/token",
        params={"grant_type": "account_credentials", "account_id": ZOOM_ACCOUNT_ID},
        auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
        timeout=15,
    )
    res.raise_for_status()
    data = res.json()
    _zoom_token_cache["token"] = data["access_token"]
    _zoom_token_cache["expires_at"] = now + data.get("expires_in", 3600)
    log.info("Zoom アクセストークンを取得しました")
    return _zoom_token_cache["token"]

def zoom_get(path: str, params: dict = None) -> dict:
    token = get_zoom_access_token()
    res = requests.get(
        f"https://api.zoom.us/v2/{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=20,
    )
    res.raise_for_status()
    return res.json()

# ==============================================================================
# ZRA: 会議一覧取得（ポーリング用）
# ==============================================================================
def get_zra_conversations(from_dt: str, to_dt: str) -> list[dict]:
    """
    GET /iq/conversations
    from_dt / to_dt: "2025-04-09T00:00:00Z" 形式
    """
    all_conversations = []
    next_page_token = ""

    while True:
        params = {
            "from": from_dt,
            "to": to_dt,
            "page_size": 50,
        }
        if next_page_token:
            params["next_page_token"] = next_page_token

        try:
            data = zoom_get("iq/conversations", params)
        except requests.HTTPError as e:
            log.error(f"ZRA conversations 取得エラー: {e}")
            break

        conversations = data.get("conversations", [])
        all_conversations.extend(conversations)
        log.info(f"ZRA: {len(conversations)} 件取得（累計: {len(all_conversations)} 件）")

        next_page_token = data.get("next_page_token", "")
        if not next_page_token:
            break

    return all_conversations

# ==============================================================================
# ZRA: 会議詳細（要約・次のステップ）取得
# ==============================================================================
def get_zra_conversation_detail(conversation_id: str) -> dict:
    """
    GET /iq/conversations/{conversationId}
    レスポンスに summary / next_steps が含まれる想定。
    ★ 実際のフィールド名はAPIレスポンスを見て要調整。
    """
    try:
        data = zoom_get(f"iq/conversations/{conversation_id}")
        return data
    except requests.HTTPError as e:
        log.error(f"ZRA 詳細取得エラー (id={conversation_id}): {e}")
        return {}

# ==============================================================================
# ZRA: 参加者情報（電話番号）取得
# ==============================================================================
def get_zra_participants(conversation_id: str) -> list[dict]:
    """
    GET /zra/conversations/{id}/interactions
    レスポンスの participants から電話番号を収集する。
    ★ 実際のフィールド構造はAPIレスポンスを見て要調整。
    """
    try:
        data = zoom_get(f"zra/conversations/{conversation_id}/interactions")
        return data.get("participants", [])
    except requests.HTTPError as e:
        log.error(f"ZRA interactions 取得エラー (id={conversation_id}): {e}")
        return []

# ==============================================================================
# 電話番号正規化
# ==============================================================================
def normalize_phone(phone: str) -> str:
    """
    記号・スペース除去 + 国際形式→国内形式変換
    例: "+81-90-1234-5678" → "09012345678"
         "090-1234-5678"   → "09012345678"
    """
    if not phone:
        return ""
    normalized = re.sub(r"[\-\(\)\+\s]", "", phone)
    # +81xx → 0xx
    if normalized.startswith("81") and len(normalized) > 10:
        normalized = "0" + normalized[2:]
    return normalized

# ==============================================================================
# 楽楽販売: 全レコードCSV取得 → 電話番号でkeyId検索
# ==============================================================================
def find_rakuraku_key_id_by_phone(target_phone: str) -> Optional[str]:
    """
    楽楽販売CSVエクスポートAPIで全件取得し、
    電話番号が一致するレコードのkeyIdを返す。
    """
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN,
    }
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "listId": RAKURAKU_LIST_ID,
        "limit": 5000,
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=30)
        if res.status_code != 200:
            log.error(f"楽楽販売CSV取得エラー: {res.status_code} {res.text}")
            return None

        csv_content = res.content.decode("utf-8-sig", errors="ignore")
        reader = csv.reader(io.StringIO(csv_content))

        # ヘッダー行を読んで電話番号列のインデックスを動的に特定
        try:
            headers_row = next(reader)
        except StopIteration:
            log.warning("楽楽販売: レコードが0件です")
            return None

        # ★ ヘッダー名は実際の楽楽販売の設定に合わせて要変更
        phone_col_candidates = ["電話番号", "TEL", "tel", "phone"]
        phone_col_idx = None
        for candidate in phone_col_candidates:
            if candidate in headers_row:
                phone_col_idx = headers_row.index(candidate)
                break

        if phone_col_idx is None:
            log.error(f"楽楽販売: 電話番号列が見つかりません。ヘッダ: {headers_row}")
            return None

        normalized_target = normalize_phone(target_phone)
        if not normalized_target:
            return None

        for row in reader:
            if len(row) <= phone_col_idx:
                continue
            key_id   = row[0].strip()
            phone_in_record = normalize_phone(row[phone_col_idx].strip())
            if phone_in_record == normalized_target:
                log.info(f"  電話番号一致: keyId={key_id} phone={phone_in_record}")
                return key_id

        log.warning(f"  楽楽販売: 電話番号 {normalized_target} に一致するレコードなし")
        return None

    except Exception as e:
        log.error(f"楽楽販売CSV検索エラー: {e}")
        return None

# ==============================================================================
# 楽楽販売: 対応履歴フィールドを更新
# ==============================================================================
def update_rakuraku_history(key_id: str, history_text: str) -> bool:
    """
    楽楽販売のレコード更新APIで対応履歴フィールドへ書き込む。
    ★ FIELD_HISTORY のフィールドIDは管理画面から確認して設定すること。
    """
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"
    headers = {
        "Content-Type": "application/json",
        "X-HD-apitoken": RAKURAKU_TOKEN,
    }
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "keyId": key_id,
        "values": {
            FIELD_HISTORY: history_text,
        },
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=10)
        if res.status_code == 200:
            log.info(f"  [楽楽販売 更新成功] keyId={key_id}")
            return True
        else:
            log.error(f"  [楽楽販売 更新失敗] keyId={key_id}: {res.status_code} {res.text}")
            return False
    except Exception as e:
        log.error(f"  [楽楽販売 更新例外] keyId={key_id}: {e}")
        return False

# ==============================================================================
# ZRAデータから要約テキストを整形
# ==============================================================================
def build_history_text(detail: dict, meeting_time: str) -> str:
    """
    ZRAのAPIレスポンスから対応履歴テキストを組み立てる。
    ★ summary / next_steps のフィールド名は実際のレスポンスに合わせて要調整。
    """
    # ── 要調整ポイント ──────────────────────────────────────
    # ZRA APIの実際のフィールド名が判明したらここを修正する。
    # 想定候補: "summary", "meetingSummary", "overview"
    summary    = detail.get("summary", "") or detail.get("meetingSummary", "")

    # 次のステップ: リスト形式の場合と文字列の場合がある
    next_steps_raw = detail.get("nextSteps", []) or detail.get("next_steps", [])
    if isinstance(next_steps_raw, list):
        next_steps_text = "\n".join(f"・{s}" for s in next_steps_raw if s)
    else:
        next_steps_text = str(next_steps_raw)
    # ────────────────────────────────────────────────────────

    lines = [f"【Zoom通話 対応履歴】{meeting_time}"]
    if summary:
        lines.append(f"\n▼ 要約\n{summary}")
    if next_steps_text:
        lines.append(f"\n▼ 次のステップ\n{next_steps_text}")
    if not summary and not next_steps_text:
        lines.append("（要約・次のステップなし）")

    return "\n".join(lines)

# ==============================================================================
# 1会議分の処理
# ==============================================================================
def process_one_conversation(conversation: dict) -> bool:
    """
    1件のZRA会議データを処理して楽楽販売へ反映する。
    Returns: 更新成功かどうか
    """
    conv_id    = conversation.get("conversationId") or conversation.get("id", "")
    start_time = conversation.get("startTime", "")
    log.info(f"--- 処理開始: conversationId={conv_id} startTime={start_time}")

    # ① ZRA詳細取得（要約・次のステップ）
    detail = get_zra_conversation_detail(conv_id)
    if not detail:
        log.warning(f"  詳細取得スキップ: {conv_id}")
        return False

    # ② 参加者の電話番号収集
    participants = get_zra_participants(conv_id)

    # ★ 電話番号フィールド名は実際のレスポンスに合わせて要調整
    phone_numbers = []
    for p in participants:
        phone = p.get("phoneNumber") or p.get("phone_number") or p.get("phone", "")
        if phone:
            phone_numbers.append(phone)

    if not phone_numbers:
        log.warning(f"  電話番号なし: conversationId={conv_id}")
        return False

    log.info(f"  参加者電話番号: {phone_numbers}")

    # ③ 楽楽販売でレコード検索（電話番号で順に試す）
    key_id = None
    matched_phone = None
    for phone in phone_numbers:
        key_id = find_rakuraku_key_id_by_phone(phone)
        if key_id:
            matched_phone = phone
            break

    if not key_id:
        log.warning(f"  楽楽販売レコード未発見: conversationId={conv_id}")
        return False

    log.info(f"  楽楽販売レコード発見: keyId={key_id} (電話番号: {matched_phone})")

    # ④ 対応履歴テキスト組み立て
    history_text = build_history_text(detail, start_time)
    log.info(f"  対応履歴テキスト:\n{history_text}")

    # ⑤ 楽楽販売へ反映
    time.sleep(1.0)  # レートリミット対策
    return update_rakuraku_history(key_id, history_text)

# ==============================================================================
# メイン（ポーリングモード）
# ==============================================================================
def main_polling():
    """定期実行（cronなど）で呼び出すポーリングモード"""
    from datetime import timedelta, timezone

    log.info("=== Zoom ZRA → 楽楽販売 対応履歴反映 開始（ポーリングモード）===")

    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    to_dt   = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    from_dt = (now - timedelta(hours=POLL_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    conversations = get_zra_conversations(from_dt, to_dt)
    log.info(f"ZRA: {len(conversations)} 件の会議を取得")

    success, skip = 0, 0
    for conv in conversations:
        result = process_one_conversation(conv)
        if result:
            success += 1
        else:
            skip += 1
        time.sleep(1.0)  # ZRA APIレートリミット対策

    log.info(f"=== 処理完了: 更新成功 {success} 件 / スキップ {skip} 件 ===")

# ==============================================================================
# メイン（Webhookモード）
# ==============================================================================
def handle_webhook_payload(payload: dict):
    """
    ZRA WebhookのPOSTボディを受け取って処理する。
    FastAPIやFlaskのエンドポイントからこの関数を呼び出す。

    期待するpayload例:
    {
      "event": "zra.conversation_completed",
      "payload": {
        "object": {
          "conversationId": "abc123",
          "startTime": "2025-04-09T10:00:00Z",
          ...
        }
      }
    }
    """
    event = payload.get("event", "")
    log.info(f"Webhook受信: event={event}")

    if event not in ("zra.conversation_completed", "recording.transcript_completed"):
        log.info(f"  対象外イベント: {event}")
        return

    conversation = payload.get("payload", {}).get("object", {})
    if not conversation:
        log.warning("  payloadにobjectが見つかりません")
        return

    process_one_conversation(conversation)

# ==============================================================================
# FastAPI Webhookサーバー（オプション）
# ==============================================================================
def create_webhook_app():
    """
    FastAPIでWebhookエンドポイントを立てる場合のサンプル。
    既存のあしなが屋チャットボットと同じサーバーに同居させる想定。

    使い方:
        uvicorn zoom_zra_to_rakuraku:app --host 0.0.0.0 --port 8001
    """
    try:
        from fastapi import FastAPI, Request, HTTPException
        import hmac, hashlib
    except ImportError:
        log.error("fastapi が未インストールです: pip install fastapi uvicorn")
        return None

    app = FastAPI()
    ZOOM_WEBHOOK_SECRET = os.environ.get("ZOOM_WEBHOOK_SECRET_TOKEN", "")

    @app.post("/webhook/zoom-zra")
    async def zoom_webhook(request: Request):
        body = await request.body()
        payload = json.loads(body)

        # Zoom Webhook URL検証（初回登録時）
        if payload.get("event") == "endpoint.url_validation":
            plain = payload["payload"]["plainToken"]
            signature = hmac.new(
                ZOOM_WEBHOOK_SECRET.encode(),
                plain.encode(),
                hashlib.sha256
            ).hexdigest()
            return {"plainToken": plain, "encryptedToken": signature}

        handle_webhook_payload(payload)
        return {"status": "ok"}

    return app

# try:でFastAPIがある場合のみappを生成
try:
    app = create_webhook_app()
except Exception:
    app = None

# ==============================================================================
# エントリーポイント
# ==============================================================================
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "webhook":
        # uvicornで起動する場合は create_webhook_app() 経由
        print("Webhookモードはuvicornで起動してください:")
        print("  uvicorn zoom_zra_to_rakuraku:app --host 0.0.0.0 --port 8001")
    else:
        main_polling()