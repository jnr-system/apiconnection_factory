"""
Zoom Revenue Accelerator (ZRA) の電話通話要約を
楽楽販売の対応履歴フィールドへ自動反映するスクリプト。

【処理フロー】
1. ZRA APIをポーリングして直近1時間の電話通話一覧を取得
2. GET /iq/conversations/{id} → 要約・参加者情報を取得
3. 楽楽販売CSVエクスポートAPIで電話番号マッチング → keyId特定
4. 楽楽販売レコード更新APIで対応履歴（明細行）へ書き込み

【設定値（要確認）】
- RAKURAKU_SCHEMA_ID: 楽楽販売のデータベースID
- RAKURAKU_LIST_ID:   レコード一覧画面設定ID
- FIELD_HISTORY:      対応履歴フィールドID（管理画面のAPI設定から確認）
- ZRA APIスコープ:    revenue_accelerator:read:admin
"""

import os
import re
import csv
import io
import time
import json
import requests
import logging
from datetime import datetime, timezone, timedelta
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
RAKURAKU_SCHEMA_ID = "101455"   # ★ 楽楽販売のデータベースID
RAKURAKU_LIST_ID   = "101517"   # ★ レコード一覧画面設定ID

# 楽楽販売フィールドID
FIELD_DATETIME     = "116128"   # 対応日時
FIELD_OPERATOR     = "116129"   # 対応者
FIELD_HISTORY      = "116130"   # 対応履歴

# ポーリングモード時の検索期間（時間）
POLL_HOURS = 1

LOG_FILE_PATH      = Path(__file__).parent / "zoom_to_rakuraku.log"
PROCESSED_IDS_PATH = Path(__file__).parent / "processed_ids.json"

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
# 処理済みID管理
# ==============================================================================

def load_processed_ids() -> dict:
    if PROCESSED_IDS_PATH.exists():
        try:
            return json.loads(PROCESSED_IDS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_processed_ids(processed: dict):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=POLL_HOURS)
    cleaned = {
        conv_id: start_time
        for conv_id, start_time in processed.items()
        if _parse_dt(start_time) >= cutoff
    }
    PROCESSED_IDS_PATH.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")
    removed = len(processed) - len(cleaned)
    if removed:
        log.info(f"処理済みID: {removed} 件の期限切れエントリを削除")

def _parse_dt(dt_str: str) -> datetime:
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

def is_already_processed(conv_id: str, processed: dict) -> bool:
    return conv_id in processed

def mark_as_processed(conv_id: str, start_time: str, processed: dict):
    processed[conv_id] = start_time

# ==============================================================================
# Zoom認証（Server-to-Server OAuth）
# ==============================================================================
_zoom_token_cache: dict = {}

def get_zoom_access_token() -> str:
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
# ZRA: 電話通話一覧取得
# ==============================================================================
def get_zra_conversations(from_dt: str, to_dt: str) -> list[dict]:
    params = {
        "from": from_dt,
        "to": to_dt,
        "page_size": 50,
        "type": "phone",
    }
    try:
        data = zoom_get("iq/conversations", params)
    except requests.HTTPError as e:
        log.error(f"ZRA conversations 取得エラー: {e}")
        return []

    conversations = data.get("conversations", [])
    log.info(f"ZRA: {len(conversations)} 件取得")
    return conversations

# ==============================================================================
# ZRA: 電話通話詳細（要約・参加者）取得
# ==============================================================================
def get_zra_conversation_detail(conversation_id: str) -> dict:
    try:
        return zoom_get(f"iq/conversations/{conversation_id}")
    except requests.HTTPError as e:
        log.error(f"ZRA 詳細取得エラー (id={conversation_id}): {e}")
        return {}

# ==============================================================================
# 電話番号正規化
# ==============================================================================
def normalize_phone(phone: str) -> str:
    """
    記号・スペース除去 + 国際形式→国内形式変換
    例: "819012345678" → "09012345678"
    """
    if not phone:
        return ""
    normalized = re.sub(r"[\-\(\)\+\s]", "", phone)
    if normalized.startswith("81") and len(normalized) > 10:
        normalized = "0" + normalized[2:]
    return normalized

# ==============================================================================
# 楽楽販売: 電話番号でkeyId検索
# ==============================================================================
def find_rakuraku_key_id_by_phone(target_phone: str) -> Optional[str]:
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN,
    }
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "listId": RAKURAKU_LIST_ID,
        "limit": 50,
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=30)
        if res.status_code != 200:
            log.error(f"楽楽販売CSV取得エラー: {res.status_code} {res.text}")
            return None

        csv_content = res.content.decode("utf-8-sig", errors="ignore")
        reader = csv.reader(io.StringIO(csv_content))

        try:
            headers_row = next(reader)
        except StopIteration:
            log.warning("楽楽販売: レコードが0件です")
            return None

        phone_col_candidates = ["電話番号_1", "電話番号_2", "電話番号_3", "電話番号", "TEL", "tel", "phone"]
        phone_col_indices = [headers_row.index(c) for c in phone_col_candidates if c in headers_row]

        if not phone_col_indices:
            log.error(f"楽楽販売: 電話番号列が見つかりません。ヘッダ: {headers_row}")
            return None

        normalized_target = normalize_phone(target_phone)
        if not normalized_target:
            return None

        for row in reader:
            key_id = row[0].strip()
            for idx in phone_col_indices:
                if len(row) <= idx:
                    continue
                phone_in_record = normalize_phone(row[idx].strip())
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
    now_jst = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"
    headers = {
        "Content-Type": "application/json",
        "X-HD-apitoken": RAKURAKU_TOKEN,
    }
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "keyId": key_id,
        "values": {
            "details": [
                {
                    FIELD_DATETIME: now_jst,
                    FIELD_OPERATOR: "zoom対応履歴",
                    FIELD_HISTORY:  history_text,
                },
            ],
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
# ZRAデータから対応履歴テキストを整形
# ==============================================================================
def build_history_text(detail: dict, meeting_time: str) -> str:
    summary = detail.get("summary", "")

    try:
        jst = timezone(timedelta(hours=9))
        dt_utc = datetime.fromisoformat(meeting_time.replace("Z", "+00:00"))
        dt_jst = dt_utc.astimezone(jst)
        meeting_time_display = dt_jst.strftime("%Y-%m-%d %H:%M")
    except Exception:
        meeting_time_display = meeting_time

    def add_linebreaks(text: str) -> str:
        return text.replace("。", "。\n")

    lines = [f"【Zoom通話 対応履歴】{meeting_time_display}"]
    if summary:
        lines.append(f"\n▼ 要約\n{add_linebreaks(summary)}")
    else:
        lines.append("（要約なし）")

    return "\n".join(lines)

# ==============================================================================
# 1会議分の処理
# ==============================================================================
def process_one_conversation(conversation: dict) -> bool:
    conv_id    = conversation.get("conversation_id", "")
    start_time = conversation.get("meeting_start_time", "")
    log.info(f"--- 処理開始: conversationId={conv_id} startTime={start_time}")

    # ① ZRA詳細取得（要約・参加者）
    detail = get_zra_conversation_detail(conv_id)
    if not detail:
        log.warning(f"  詳細取得スキップ: {conv_id}")
        return False

    # ② 参加者から customer の電話番号を収集
    phone_numbers = []
    for p in detail.get("participants", []):
        if p.get("type") == "customer":
            phone = p.get("display_name", "")
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
    time.sleep(1.0)
    return update_rakuraku_history(key_id, history_text)

# ==============================================================================
# メイン（ポーリングモード）
# ==============================================================================
def main_polling():
    log.info("=== Zoom ZRA → 楽楽販売 対応履歴反映 開始（ポーリングモード）===")

    now     = datetime.now(timezone.utc)
    to_dt   = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    from_dt = (now - timedelta(hours=POLL_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    conversations = get_zra_conversations(from_dt, to_dt)
    log.info(f"ZRA: {len(conversations)} 件の会議を取得")

    processed = load_processed_ids()
    success, skip, duplicate = 0, 0, 0
    for conv in conversations:
        conv_id    = conv.get("conversation_id", "")
        start_time = conv.get("meeting_start_time", "")

        if conv.get("processing_analysis", False):
            log.info(f"  スキップ（AI処理中）: conversationId={conv_id}")
            skip += 1
            continue

        if is_already_processed(conv_id, processed):
            log.info(f"  スキップ（処理済み）: conversationId={conv_id}")
            duplicate += 1
            continue

        result = process_one_conversation(conv)
        if result:
            mark_as_processed(conv_id, start_time, processed)
            success += 1
        else:
            skip += 1
        time.sleep(1.0)

    save_processed_ids(processed)
    log.info(f"=== 処理完了: 更新成功 {success} 件 / スキップ {skip} 件 / 処理済みスキップ {duplicate} 件 ===")

# ==============================================================================
# エントリーポイント
# ==============================================================================
if __name__ == "__main__":
    main_polling()
