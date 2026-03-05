"""
GCS 画像アップロード・閲覧 FastAPI アプリ

エンドポイント:
  POST /upload/{inquiry_id}  - GCS へ画像をアップロード
  GET  /view/{inquiry_id}    - GCS 上の画像を署名付き URL で閲覧
"""

import csv
import io
import json
import os
from pathlib import Path
from typing import Annotated, List, Optional
from urllib.parse import quote

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 本番環境では python-dotenv がなくても動作する

import requests
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from google.cloud import storage
from google.oauth2 import service_account

# ──────────────────────────────────────────────
# 設定（環境変数から読み込み）
# ──────────────────────────────────────────────

# GCS バケット名
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "rakuraku-photos_factory-jnr")

# 署名付き URL の有効期間 (秒)
SIGNED_URL_EXPIRATION = 3600  # 1 時間

# GCS 上の画像フォルダプレフィックス
GCS_PREFIX = "photos"

# ──────────────────────────────────────────────
# 楽楽販売 設定（環境変数から読み込み）
# ──────────────────────────────────────────────

# 楽楽販売 接続情報（既存スクリプトと共通）
RAKURAKU_DOMAIN  = os.environ.get("RAKURAKU_DOMAIN", "hntobias.rakurakuhanbai.jp")
RAKURAKU_ACCOUNT = os.environ.get("RAKURAKU_ACCOUNT", "mspy4wa")
RAKURAKU_TOKEN   = os.environ["RAKURAKU_TOKEN"]

# 問い合わせフォーム DB 情報
RAKURAKU_INQUIRY_DB_ID     = os.environ.get("RAKURAKU_INQUIRY_DB_ID", "101443")
RAKURAKU_INQUIRY_SEARCH_ID = os.environ.get("RAKURAKU_INQUIRY_SEARCH_ID", "107323")
RAKURAKU_INQUIRY_LIST_ID   = os.environ.get("RAKURAKU_INQUIRY_LIST_ID", "101471")

# 楽楽販売の「画像用URL」項目に書き込む
RAKURAKU_URL_FIELD    = "画像用URL"  # 表示名
RAKURAKU_URL_FIELD_ID = os.environ.get("RAKURAKU_URL_FIELD_ID", "115927")  # APIパラメータ情報のフィールドID

# このサーバーのベースURL（楽楽販売に書き込む閲覧ページのURL生成に使用）
# ConoHa VPS の IP またはドメインを SERVER_BASE_URL 環境変数に設定してください
SERVER_BASE_URL = os.environ.get("SERVER_BASE_URL", "http://127.0.0.1:8000")

# ──────────────────────────────────────────────
# GCS クライアントの初期化
# ──────────────────────────────────────────────

def _get_gcs_credentials() -> service_account.Credentials:
    """GCS 認証情報を取得する。
    優先順位:
    1. 環境変数 GOOGLE_SERVICE_ACCOUNT_JSON（JSON文字列）
    2. 同フォルダの google_secret.json ファイル
    """
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        # ローカル開発用：google_secret.json を自動探索
        secret_path = Path(__file__).parent / "google_secret.json"
        if secret_path.exists():
            raw = secret_path.read_text(encoding="utf-8")
        else:
            raise RuntimeError(
                "GCS認証情報が見つかりません。\n"
                "・環境変数 GOOGLE_SERVICE_ACCOUNT_JSON を設定するか、\n"
                f"・{secret_path} を配置してください。"
            )
    creds_dict = json.loads(raw)
    return service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def get_storage_client() -> storage.Client:
    """サービスアカウント情報から GCS クライアントを生成する。"""
    credentials = _get_gcs_credentials()
    return storage.Client(credentials=credentials, project=credentials.project_id)


def get_bucket() -> storage.Bucket:
    """バケットオブジェクトを返す。
    ※ client.bucket() はローカルオブジェクト生成のみで API を呼ばないため
      storage.buckets.get 権限が不要。実際のエラーは upload/list 時に検出される。
    """
    client = get_storage_client()
    return client.bucket(BUCKET_NAME)


# ──────────────────────────────────────────────
# 楽楽販売 連携関数
# ──────────────────────────────────────────────

def _rakuraku_headers() -> dict:
    # 既存スクリプトの形式に合わせ Content-Type に charset なし
    return {
        "Content-Type": "application/json",
        "X-HD-apitoken": RAKURAKU_TOKEN,
    }


def find_rakuraku_record_id(phone: str) -> Optional[str]:
    """
    楽楽販売の問い合わせDBから電話番号が一致するレコードIDを返す。
    見つからない場合は None を返す。
    電話番号_1 または 電話番号_2 で検索し、最新登録順（先頭）を返す。
    """
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
        raise HTTPException(status_code=502, detail=f"楽楽販売 CSV取得エラー: {e}")

    # デコード（cp932 → utf-8 フォールバック）
    try:
        csv_text = res.content.decode("cp932")
    except Exception:
        csv_text = res.content.decode("utf-8", errors="ignore")

    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if len(rows) < 2:
        return None

    header = rows[0]

    # レコードIDの列を探す（楽楽販売は先頭列が「ID」のことが多い）
    def col(name: str) -> int:
        for i, h in enumerate(header):
            if name in h:
                return i
        return -1

    idx_id   = col("ID")
    idx_tel  = col("電話番号")
    idx_name = col("名前")

    if idx_id == -1:
        raise HTTPException(status_code=500, detail="楽楽販売のCSVに『ID』列が見つかりません。")

    # 電話番号の正規化（ハイフン除去）
    phone_norm = phone.replace("-", "").replace("ー", "").replace("−", "")

    for row in rows[1:]:
        tel = row[idx_tel].replace("-", "") if idx_tel != -1 and len(row) > idx_tel else ""
        if phone_norm == tel:
            key_id = row[idx_id].strip()
            name   = row[idx_name].strip() if idx_name != -1 and len(row) > idx_name else ""
            return {"key_id": key_id, "name": name}

    return None  # 一致なし


def update_rakuraku_photo_url(key_id: str, view_url: str) -> dict:
    """
    キー項目の値（key_id）でレコードを特定し、「画像用URL」フィールドに閲覧URLを書き込む。
    既存の RakuRakudata_update_ver3.py と同じ形式を使用。
    """
    # ★ 正しいURL（/api/ なし）→ 既存スクリプトと同じパターン
    url = f"https://{RAKURAKU_DOMAIN}/{RAKURAKU_ACCOUNT}/apirecord/update/version/v1"
    payload = {
        "dbSchemaId": RAKURAKU_INQUIRY_DB_ID,
        "keyId":      key_id,      # ★ id → keyId （キー項目の値）
        "values": {
            RAKURAKU_URL_FIELD_ID: view_url,  # フィールドID: 115927（画像用URL）
        },
    }
    try:
        res = requests.post(url, headers=_rakuraku_headers(), json=payload, timeout=30)
        print(f"[楽楽販売 更新API] status={res.status_code}, body={res.text[:500]}")
        print(f"[楽楽販売 更新API] payload={payload}")
        res.raise_for_status()
        return {"success": True, "key_id": key_id, "url_written": view_url}
    except requests.HTTPError:
        raise HTTPException(
            status_code=502,
            detail=f"楽楽販売 更新APIエラー (key_id={key_id}): "
                   f"HTTP {res.status_code} / レスポンス: {res.text[:400]}",
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"楽楽販売 更新エラー: {e}")

# ──────────────────────────────────────────────
# FastAPI アプリ
# ──────────────────────────────────────────────

app = FastAPI(
    title="給湯器設置写真 管理 API",
    description="GCS を使った画像アップロード・閲覧エンドポイント",
    version="1.0.0",
)


# ──────────────────────────────────────────────
# デバッグ: 楽楽販売 CSV ヘッダー確認
# ──────────────────────────────────────────────

@app.get(
    "/debug/fields",
    summary="楽楽販売の列ヘッダー確認",
    description="問い合わせDB(101443)のCSVヘッダーを返します。フィールドID確認用。",
)
async def debug_fields():
    """CSVの1行目（列名）を返してフィールド構成を確認するデバッグ用エンドポイント。"""
    url = f"https://{RAKURAKU_DOMAIN}/{RAKURAKU_ACCOUNT}/api/csvexport/version/v1"
    payload = {
        "dbSchemaId": RAKURAKU_INQUIRY_DB_ID,
        "listId":     RAKURAKU_INQUIRY_LIST_ID,
        "searchId":   RAKURAKU_INQUIRY_SEARCH_ID,
        "limit":      1,
    }
    try:
        res = requests.post(url, headers=_rakuraku_headers(), json=payload, timeout=30)
        res.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"CSV取得エラー: {e}")

    try:
        csv_text = res.content.decode("cp932")
    except Exception:
        csv_text = res.content.decode("utf-8", errors="ignore")

    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return {"headers": [], "note": "データが0件です"}

    return {
        "headers": rows[0],
        "sample_row": rows[1] if len(rows) > 1 else [],
        "note": "↑ の列名を使って /debug/record/{key_id} でフィールドIDを確認してください",
    }


@app.get(
    "/debug/record/{key_id}",
    summary="楽楽販売のレコードをJSON取得（フィールドID確認用）",
    description="指定した記録IDのレコードをAPIから取得し、フィールドIDと値の対応を返します。",
)
async def debug_record(key_id: str):
    """
    楽楽販売のレコード取得APIを呼び出し、フィールドID ↔ 表示名の対応を確認する。
    取得したJSONに含まれるフィールドIDを RAKURAKU_URL_FIELD_ID に設定してください。
    """
    # レコード取得API（URLパターンはCSVexportと同系統）
    for path in [
        f"https://{RAKURAKU_DOMAIN}/{RAKURAKU_ACCOUNT}/apirecord/get/version/v1",
        f"https://{RAKURAKU_DOMAIN}/{RAKURAKU_ACCOUNT}/api/apirecord/get/version/v1",
    ]:
        try:
            res = requests.post(
                path,
                headers=_rakuraku_headers(),
                json={"dbSchemaId": RAKURAKU_INQUIRY_DB_ID, "keyId": key_id},
                timeout=15,
            )
            print(f"[debug/record] {path} → {res.status_code}: {res.text[:300]}")
            if res.status_code == 200:
                data = res.json()
                return {"raw_response": data, "path_used": path}
        except Exception as e:
            print(f"[debug/record] {path} → error: {e}")

    raise HTTPException(
        status_code=502,
        detail=(
            "レコード取得APIが利用できませんでした。\n"
            "楽楽販売 管理者設定 → メンテナンス機能 → APIパラメータ情報 → "
            "DB 101443 を選択して『画像用URL』のIDを確認してください。"
        ),
    )


# ──────────────────────────────────────────────
# お問い合わせフォーム（正直屋フォームを再現 + 画像アップロード）
# ──────────────────────────────────────────────

@app.get(
    "/contact",
    response_class=HTMLResponse,
    summary="お問い合わせフォーム（テスト）",
    description="正直屋の問い合わせフォームを再現したテストページ。設置写真アップロード付き。",
)
async def contact_form():
    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>無料お見積り | 正直屋（テスト）</title>
    <style>
        *, *::before, *::after { box-sizing: border-box; }
        body { font-family: 'Helvetica Neue', sans-serif; background: #f5f5f5; margin: 0; padding: 0; color: #333; }
        header { background: #1a3a6b; color: #fff; padding: 16px 24px; display: flex; align-items: center; gap: 12px; }
        header h1 { font-size: 1.2rem; margin: 0; }
        header span { background: #e63946; color: #fff; font-size: 0.7rem; padding: 3px 8px; border-radius: 4px; }
        .container { max-width: 700px; margin: 32px auto; padding: 0 16px 48px; }
        h2 { color: #1a3a6b; border-left: 4px solid #e63946; padding-left: 12px; margin-top: 32px; }
        .card { background: #fff; border-radius: 10px; padding: 28px; box-shadow: 0 2px 10px rgba(0,0,0,.08); }
        .field { margin-bottom: 20px; }
        label { display: block; font-weight: bold; font-size: 0.9rem; margin-bottom: 6px; color: #444; }
        label .req { color: #e63946; margin-left: 4px; font-size: 0.8rem; }
        input[type=text], input[type=email], input[type=tel], textarea, select {
            width: 100%; padding: 10px 12px; border: 1px solid #ccc; border-radius: 6px;
            font-size: 0.95rem; outline: none; transition: border .2s;
        }
        input:focus, textarea:focus, select:focus { border-color: #1a3a6b; }
        textarea { min-height: 100px; resize: vertical; }
        .upload-area {
            border: 2px dashed #1a3a6b; border-radius: 8px; padding: 24px;
            text-align: center; cursor: pointer; background: #f0f4ff;
            transition: background .2s;
        }
        .upload-area:hover { background: #dde8ff; }
        .upload-area input { display: none; }
        .upload-area p { color: #1a3a6b; margin: 0; font-size: 0.95rem; }
        #file-list { margin-top: 10px; font-size: 0.87rem; color: #555; }
        .privacy { font-size: 0.85rem; color: #666; margin: 16px 0; }
        .privacy a { color: #1a3a6b; }
        .btn {
            display: block; width: 100%; padding: 14px;
            background: #e63946; color: #fff; border: none; border-radius: 8px;
            font-size: 1.05rem; font-weight: bold; cursor: pointer; transition: background .2s;
        }
        .btn:hover { background: #c1121f; }
        #result { margin-top: 20px; padding: 16px; border-radius: 8px; display: none; font-size: 0.92rem; }
        .success { background: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7; }
        .error   { background: #ffebee; color: #c62828; border: 1px solid #ef9a9a; }
        .view-link { display: block; margin-top: 10px; color: #1a3a6b; font-weight: bold; text-decoration: none; }
        .note { font-size: 0.8rem; color: #888; margin-top: 4px; }
    </style>
</head>
<body>
<header>
    <h1>🔧 正直屋 無料お見積り</h1>
    <span>テスト環境</span>
</header>
<div class="container">
    <h2>📋 お問い合わせフォーム</h2>
    <div class="card">
        <div class="field">
            <label>お名前 <span class="req">必須</span></label>
            <input type="text" id="name" placeholder="例：山田 太郎" />
        </div>
        <div class="field">
            <label>メールアドレス <span class="req">必須</span></label>
            <input type="email" id="email" placeholder="例：yamada@example.com" />
            <p class="note">※携帯の迷惑メール設定で受信できない場合があります。</p>
        </div>
        <div class="field">
            <label>ご住所</label>
            <input type="text" id="address" placeholder="例：愛知県名古屋市千種区〇〇町1-2-3" />
        </div>
        <div class="field">
            <label>電話番号 <span class="req">必須</span></label>
            <input type="tel" id="phone" placeholder="例：090-1234-5678" />
        </div>
        <div class="field">
            <label>現在お使いの給湯器またはガスコンロの品番</label>
            <input type="text" id="model" placeholder="不明の場合は「不明」とご記入ください" />
        </div>
        <div class="field">
            <label>ご使用の機器の状況</label>
            <textarea id="status" placeholder="例：お湯が出なくなった、エラーコードが表示されている等"></textarea>
        </div>
        <div class="field">
            <label>ガスの種類</label>
            <select id="gas">
                <option value="">選択してください</option>
                <option value="都市ガス">都市ガス（12A・13A）</option>
                <option value="プロパン">LPガス（プロパン）</option>
                <option value="不明">不明</option>
            </select>
        </div>
        <div class="field">
            <label>ご質問・ご要望</label>
            <textarea id="message" placeholder="自由にご記入ください"></textarea>
        </div>

        <h2 style="margin-top:28px;">📷 設置写真のアップロード（任意）</h2>
        <div class="field">
            <div class="upload-area" onclick="document.getElementById('photos').click()">
                <p>📁 クリックして写真を選択<br><span style="font-size:.82rem;color:#666">（複数枚選択可・JPEG/PNG等）</span></p>
                <input type="file" id="photos" multiple accept="image/*" onchange="showFiles(this)" />
            </div>
            <div id="file-list"></div>
        </div>

        <div class="privacy">
            <input type="checkbox" id="agree" />
            <label for="agree" style="font-weight:normal;display:inline;">
                <a href="https://syouzikiya.jp/privacy/" target="_blank">個人情報保護について</a>を読み、同意しました
            </label>
        </div>

        <button class="btn" onclick="submitForm()">送信する</button>
        <div id="result"></div>
    </div>
</div>

<script>
function showFiles(input) {
    const list = document.getElementById('file-list');
    list.textContent = [...input.files].map(f => '📎 ' + f.name).join('  ');
}

async function submitForm() {
    const name  = document.getElementById('name').value.trim();
    const phone = document.getElementById('phone').value.trim();
    const email = document.getElementById('email').value.trim();
    const files = document.getElementById('photos').files;

    if (!name)  return showResult('error', 'お名前を入力してください。');
    if (!phone) return showResult('error', '電話番号を入力してください。');
    if (!email) return showResult('error', 'メールアドレスを入力してください。');
    if (!document.getElementById('agree').checked)
        return showResult('error', '個人情報保護について同意してください。');

    const fd = new FormData();
    fd.append('name',    name);
    fd.append('phone',   phone);
    fd.append('email',   email);
    fd.append('address', document.getElementById('address').value.trim());
    fd.append('model',   document.getElementById('model').value.trim());
    fd.append('status',  document.getElementById('status').value.trim());
    fd.append('gas',     document.getElementById('gas').value);
    fd.append('message', document.getElementById('message').value.trim());
    for (const f of files) fd.append('files', f);

    showResult('success', '送信中...', null);

    try {
        const res  = await fetch('/contact/submit', { method: 'POST', body: fd });
        const data = await res.json();
        if (res.ok) {
            let msg = `✅ 送信完了しました！${name} 様のお問い合わせを受け付けました。`;
            if (data.view_url) msg += `<br>📷 アップロードされた写真：`;
            if (data.rakuraku?.success) msg += `<br>📔 楽楽販売レコード（ID: ${data.rakuraku.key_id}）を更新しました。`;
            if (data.rakuraku?.skipped) msg += `<br>⚠️ 楽楽販売: ${data.rakuraku.reason}`;
            showResult('success', msg, data.view_url);
        } else {
            showResult('error', '❌ エラー: ' + (data.detail || '送信に失敗しました'));
        }
    } catch(e) {
        showResult('error', '❌ 通信エラー: ' + e);
    }
}

function showResult(type, msg, viewUrl) {
    const d = document.getElementById('result');
    d.className = type; d.style.display = 'block';
    d.innerHTML = msg;
    if (viewUrl) d.innerHTML += `<a class="view-link" href="${viewUrl}" target="_blank">🖼️ 写真を確認する →</a>`;
}
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.post(
    "/contact/submit",
    summary="お問い合わせ送信処理",
    description="フォームデータを受け取り、画像をGCSへアップロードし楽楽販売レコードを更新する。",
)
async def contact_submit(
    name:    Annotated[str, Form()],
    phone:   Annotated[str, Form()],
    email:   Annotated[str, Form()],
    address: Annotated[Optional[str], Form()] = None,
    model:   Annotated[Optional[str], Form()] = None,
    status:  Annotated[Optional[str], Form()] = None,
    gas:     Annotated[Optional[str], Form()] = None,
    message: Annotated[Optional[str], Form()] = None,
    files:   Annotated[Optional[List[UploadFile]], File()] = None,
):
    """
    フォーム送信処理（テスト環境用）
    1. 顧客名をフォルダとしてGCSに画像をアップロード
    2. 電話番号で楽楽販売レコードを検索してURLを書き込む
    """
    # 電話番号正規化（ハイフン除去 → 楽楽販売の自動処理と一致させる）
    phone_clean = phone.replace("-", "").replace("ー", "").replace("−", "")

    # ── 画像アップロード ──
    uploaded = []
    folder_name = name  # 顧客名をフォルダ名に使用
    if files:
        bucket = get_bucket()
        for upload_file in files:
            filename = upload_file.filename
            if not filename:
                continue
            blob_name = f"{GCS_PREFIX}/{folder_name}/{filename}"
            blob = bucket.blob(blob_name)
            try:
                content = await upload_file.read()
                blob.upload_from_string(
                    content,
                    content_type=upload_file.content_type or "application/octet-stream",
                )
                uploaded.append(blob_name)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"'{filename}' のアップロードに失敗: {e}")

    # ── 楽楽販売連携（電話番号で検索 → URL書き込み）──
    rakuraku_result = None
    view_url = None
    if uploaded:
        encoded_folder = quote(folder_name, safe="")
        view_url = f"{SERVER_BASE_URL}/view/{encoded_folder}"

        rakuraku_info = find_rakuraku_record_id(phone_clean)
        if rakuraku_info:
            rakuraku_result = update_rakuraku_photo_url(rakuraku_info["key_id"], view_url)
        else:
            rakuraku_result = {
                "skipped": True,
                "reason": f"電話番号 '{phone}' ({phone_clean}) に一致するレコードが見つかりませんでした（レコード登録後に再試行してください）。"
            }

    return {
        "message":  f"受付完了。アップロード: {len(uploaded)} 件",
        "name":     name,
        "phone":    phone,
        "uploaded": uploaded,
        "view_url": view_url,
        "rakuraku": rakuraku_result,
    }


# ──────────────────────────────────────────────
# アップロードエンドポイント
# ──────────────────────────────────────────────

@app.post(
    "/upload/{inquiry_id}",
    summary="画像をアップロード",
    description=(
        "inquiry_id ごとの GCS フォルダ (photos/{inquiry_id}/) に "
        "複数の画像ファイルをアップロードします。"
    ),
)
async def upload_images(
    inquiry_id: str,
    files: Annotated[List[UploadFile], File(description="アップロードする画像ファイル（複数可）")],
    phone: Annotated[Optional[str], Form(description="楽楽販売のレコードと結び付ける電話番号（任意）")] = None,
):
    if not files:
        raise HTTPException(status_code=400, detail="ファイルが選択されていません。")

    bucket = get_bucket()
    uploaded = []

    # ── 電話番号があれば楽楽販売から顧客名を事前取得してフォルダ名を決定 ──
    folder_name  = inquiry_id   # デフォルト: フォームの inquiry_id
    rakuraku_info = None
    if phone and phone.strip():
        rakuraku_info = find_rakuraku_record_id(phone.strip())
        if rakuraku_info and rakuraku_info.get("name"):
            folder_name = rakuraku_info["name"]           # 顧客名をフォルダに使用
        elif phone:
            folder_name = phone.strip().replace("-", "")  # 電話番号フォールバック

    for upload_file in files:
        filename = upload_file.filename
        if not filename:
            raise HTTPException(status_code=400, detail="ファイル名が空のファイルが含まれています。")

        blob_name = f"{GCS_PREFIX}/{folder_name}/{filename}"
        blob = bucket.blob(blob_name)

        try:
            content = await upload_file.read()
            blob.upload_from_string(
                content,
                content_type=upload_file.content_type or "application/octet-stream",
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"'{filename}' のアップロードに失敗しました: {e}",
            )

        uploaded.append(blob_name)

    # ── 楽楽販売連携：取得済みレコード情報でURLを書き込む ──
    rakuraku_result = None
    if rakuraku_info:
        # 日本語フォルダ名をURLエンコードして楽楽販売のURL形式チェックをパスさせる
        encoded_folder = quote(folder_name, safe="")
        view_url = f"{SERVER_BASE_URL}/view/{encoded_folder}"
        rakuraku_result = update_rakuraku_photo_url(rakuraku_info["key_id"], view_url)
    elif phone and phone.strip():
        rakuraku_result = {
            "skipped": True,
            "reason": f"電話番号 '{phone}' に一致する楽楽販売のレコードが見つかりませんでした。"
        }

    return {
        "message": f"{len(uploaded)} 件のファイルをアップロードしました。",
        "inquiry_id": inquiry_id,
        "uploaded_files": uploaded,
        "rakuraku": rakuraku_result,
    }


# ──────────────────────────────────────────────
# アップロードフォーム画面
# ──────────────────────────────────────────────

@app.get(
    "/form",
    response_class=HTMLResponse,
    summary="アップロードフォーム画面",
    description="ブラウザから画像をアップロードできる HTML フォームページを返します。",
)
async def upload_form():
    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>設置写真アップロード</title>
    <style>
        body { font-family: sans-serif; background: #f0f4f8; margin: 0; padding: 40px; }
        .card {
            background: #fff; border-radius: 12px; padding: 32px;
            max-width: 520px; margin: 0 auto;
            box-shadow: 0 4px 16px rgba(0,0,0,.12);
        }
        h1 { font-size: 1.4rem; color: #1a1a2e; margin-bottom: 24px; }
        label { display: block; font-size: 0.88rem; color: #444; margin-bottom: 6px; font-weight: bold; }
        input[type="text"], input[type="file"] {
            width: 100%; box-sizing: border-box;
            padding: 10px 12px; border: 1px solid #ccc;
            border-radius: 8px; font-size: 0.95rem; margin-bottom: 20px;
        }
        input[type="file"] { padding: 6px; cursor: pointer; }
        button {
            background: #0078d4; color: #fff; border: none;
            border-radius: 8px; padding: 12px 28px;
            font-size: 1rem; cursor: pointer; width: 100%;
        }
        button:hover { background: #005fa3; }
        #result {
            margin-top: 20px; padding: 14px; border-radius: 8px;
            display: none; font-size: 0.9rem;
        }
        .success { background: #e6f4ea; color: #1e7e34; border: 1px solid #a8d5b5; }
        .error   { background: #fdecea; color: #b71c1c; border: 1px solid #f5c6c6; }
        .view-link { margin-top: 12px; display: block; color: #0078d4; text-decoration: none; font-weight: bold; }
    </style>
</head>
<body>
    <div class="card">
        <h1>📷 給湯器設置写真 アップロード</h1>
        <label for="inquiry_id">問い合わせアイディファイアー（inquiry_id）</label>
        <input type="text" id="inquiry_id" placeholder="例: INQ-001" />

        <label for="phone">電話番号（楽楽販売連携用）<span style="font-weight:normal;color:#888">▼任意</span></label>
        <input type="text" id="phone" placeholder="例: 090-1234-5678" />

        <label for="files">画像ファイルを選択（複数可）</label>
        <input type="file" id="files" multiple accept="image/*" />

        <button onclick="uploadFiles()">アップロード</button>
        <div id="result"></div>
    </div>

    <script>
    async function uploadFiles() {
        const inquiryId = document.getElementById('inquiry_id').value.trim();
        const files = document.getElementById('files').files;
        const resultDiv = document.getElementById('result');

        if (!inquiryId) {
            showResult('error', '問い合わせIDを入力してください。');
            return;
        }
        if (files.length === 0) {
            showResult('error', 'ファイルを選択してください。');
            return;
        }

        const formData = new FormData();
        for (const file of files) {
            formData.append('files', file);
        }
        const phone = document.getElementById('phone').value.trim();
        if (phone) formData.append('phone', phone);

        showResult('success', 'アップロード中...', null);

        try {
            const res = await fetch(`/upload/${encodeURIComponent(inquiryId)}`, {
                method: 'POST', body: formData
            });
            const data = await res.json();
            if (res.ok) {
                let msg = `✅ ${data.uploaded_files.length} 件アップロード完了！`;
                if (data.rakuraku) {
                    if (data.rakuraku.success) {
                        msg += `<br>📔 楽楽販売のレコード（ID: ${data.rakuraku.record_id}）にURLを書き込みました。`;
                    } else if (data.rakuraku.skipped) {
                        msg += `<br>⚠️ 楽楽販売連携: ${data.rakuraku.reason}`;
                    }
                }
                showResult('success', msg, `/view/${encodeURIComponent(inquiryId)}`);
            } else {
                showResult('error', `❌ エラー: ${data.detail}`);
            }
        } catch (e) {
            showResult('error', `❌ 通信エラー: ${e}`);
        }
    }

    function showResult(type, msg, viewUrl) {
        const d = document.getElementById('result');
        d.className = type;
        d.style.display = 'block';
        d.innerHTML = msg;
        if (viewUrl) {
            d.innerHTML += `<a class="view-link" href="${viewUrl}" target="_blank">📄 この問い合わせの写真を見る →</a>`;
        }
    }
    </script>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)


# ──────────────────────────────────────────────
# 閲覧エンドポイント
# ──────────────────────────────────────────────

@app.get(
    "/view/{inquiry_id}",
    response_class=HTMLResponse,
    summary="画像を閲覧",
    description=(
        "inquiry_id に対応する GCS フォルダ内の全画像を "
        "署名付き URL (1 時間有効) で表示する HTML ページを返します。"
    ),
)
async def view_images(inquiry_id: str):
    bucket = get_bucket()
    prefix = f"{GCS_PREFIX}/{inquiry_id}/"

    # ファイル一覧を取得
    try:
        blobs = list(bucket.list_blobs(prefix=prefix))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"ファイル一覧の取得に失敗しました: {e}",
        )

    # フォルダ自体 (末尾が /) を除外し、実ファイルのみ対象にする
    blobs = [b for b in blobs if not b.name.endswith("/")]

    if not blobs:
        raise HTTPException(
            status_code=404,
            detail=(
                f"inquiry_id='{inquiry_id}' に対応する画像が見つかりません。"
                f" (GCS プレフィックス: {prefix})"
            ),
        )

    # 署名付き URL を生成（サービスアカウント認証情報を使用）
    credentials = _get_gcs_credentials()

    signed_urls = []
    for blob in blobs:
        try:
            url = blob.generate_signed_url(
                version="v4",
                expiration=SIGNED_URL_EXPIRATION,
                method="GET",
                credentials=credentials,
            )
            signed_urls.append((blob.name.split("/")[-1], url))
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"署名付き URL の生成に失敗しました ({blob.name}): {e}",
            )

    # シンプルな HTML を生成
    img_tags = "\n".join(
        f"""
        <div class="photo-block">
            <p class="filename">{fname}</p>
            <img src="{url}" alt="{fname}" />
        </div>"""
        for fname, url in signed_urls
    )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>設置写真 - 問い合わせID: {inquiry_id}</title>
    <style>
        body {{
            font-family: sans-serif;
            background: #f4f4f4;
            margin: 0;
            padding: 20px;
        }}
        h1 {{
            font-size: 1.4rem;
            color: #333;
            border-bottom: 2px solid #0078d4;
            padding-bottom: 8px;
        }}
        .photo-block {{
            margin-bottom: 30px;
            background: #fff;
            border-radius: 8px;
            padding: 12px;
            box-shadow: 0 2px 6px rgba(0,0,0,.12);
            max-width: 900px;
        }}
        .filename {{
            font-size: 0.85rem;
            color: #555;
            margin: 0 0 8px 0;
        }}
        img {{
            max-width: 100%;
            height: auto;
            border-radius: 4px;
            display: block;
        }}
        .count {{
            font-size: 0.9rem;
            color: #666;
            margin-bottom: 20px;
        }}
    </style>
</head>
<body>
    <h1>📷 設置写真 — 問い合わせID: {inquiry_id}</h1>
    <p class="count">画像数: {len(signed_urls)} 枚（リンクの有効期限: 1 時間）</p>
    {img_tags}
</body>
</html>"""

    return HTMLResponse(content=html, status_code=200)


# ──────────────────────────────────────────────
# ローカル起動用
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
