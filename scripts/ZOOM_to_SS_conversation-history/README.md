# ZOOM_to_RR_sync

## 概要

Zoom Revenue Accelerator (ZRA) の会議要約・次のステップを楽楽販売の対応履歴フィールドへ自動反映するスクリプトです。

**ポーリングモード**（定期実行）と**Webhookモード**（FastAPI）の2つの動作モードに対応しています。

---

## 処理の流れ

```
Zoom ZRA API
   ↓ GET /iq/conversations → 直近1時間の会議一覧取得
   ↓ GET /iq/conversations/{id} → 要約・次のステップ取得
   ↓ GET /zra/conversations/{id}/interactions → 参加者の電話番号取得
   ↓ 電話番号を正規化（国際形式→国内形式）
楽楽販売（CSVエクスポートAPI）
   ↓ 全レコードから電話番号が一致するレコードのkeyIdを検索
楽楽販売（レコード更新API）
   ↓ 対応履歴フィールドへ要約テキストを書き込み
完了ログ出力
```

---

## 詳細な処理内容

### 1. Zoom認証（Server-to-Server OAuth）

- `ZOOM_ACCOUNT_ID` / `ZOOM_CLIENT_ID` / `ZOOM_CLIENT_SECRET` でアクセストークンを取得
- トークンは1時間キャッシュして再利用

### 2. ZRA会議データ取得

- `GET /iq/conversations` で直近 `POLL_HOURS`（デフォルト: 1時間）の会議一覧を取得（ページング対応）
- `GET /iq/conversations/{id}` で要約（`summary`）・次のステップ（`nextSteps`）を取得
- `GET /zra/conversations/{id}/interactions` で参加者の電話番号を取得

### 3. 電話番号正規化

記号・スペースを除去し、国際形式（`+81`）を国内形式（`0`始まり）に変換してマッチングに使用。

### 4. 楽楽販売でレコード検索

参加者の電話番号を順に試し、楽楽販売のCSVエクスポートAPIで全件検索してkeyIdを特定。

### 5. 対応履歴テキスト組み立て・書き込み

```
【Zoom通話 対応履歴】{通話日時}

▼ 要約
{summary}

▼ 次のステップ
・{next_step_1}
・{next_step_2}
```

上記フォーマットで楽楽販売の対応履歴フィールドへ書き込む。

---

## 設定パラメータ（要変更箇所）

| 変数名 | 説明 |
|---|---|
| `RAKURAKU_SCHEMA_ID` | 楽楽販売のデータベースID（管理画面から確認） |
| `RAKURAKU_LIST_ID` | レコード一覧画面設定ID（管理画面から確認） |
| `FIELD_PHONE` | 電話番号フィールドID（管理画面のAPI設定から確認） |
| `FIELD_HISTORY` | 対応履歴フィールドID（管理画面のAPI設定から確認） |
| `POLL_HOURS` | ポーリング時の検索期間（時間）。デフォルト: `1` |

> ZRA APIのレスポンス構造（`summary` / `nextSteps` / `phoneNumber` のフィールド名）は実際のAPIレスポンスを確認して `build_history_text()` / `get_zra_participants()` を調整してください。

---

## ファイル構成

| ファイル | 説明 |
|---|---|
| `zoom_to_RR_conversation-history.py` | メインスクリプト |
| `zoom-to-rr-sync.service` | systemd サービスユニットファイル |
| `zoom-to-rr-sync.timer` | systemd タイマーユニットファイル（1時間毎） |
| `.env` | ローカル開発用の環境変数ファイル（Git管理外） |

---

## 環境変数（必須）

| 変数名 | 説明 |
|---|---|
| `ZOOM_ACCOUNT_ID` | Zoom Server-to-Server OAuthのアカウントID |
| `ZOOM_CLIENT_ID` | Zoom OAuthアプリのクライアントID |
| `ZOOM_CLIENT_SECRET` | Zoom OAuthアプリのクライアントシークレット |
| `RAKURAKU_TOKEN` | 楽楽販売 APIトークン |
| `ZOOM_WEBHOOK_SECRET_TOKEN` | Webhook署名検証用シークレット（Webhookモード時のみ） |

> ローカル実行時は同フォルダの `.env` ファイルに記述してください。
> 本番環境では `/etc/apiconnection.env` に設定してください。

---

## 実行方法

### ポーリングモード（定期実行）

```bash
python zoom_to_RR_conversation-history.py
```

### Webhookモード（FastAPIサーバー）

```bash
pip install fastapi uvicorn
uvicorn zoom_to_RR_conversation-history:app --host 0.0.0.0 --port 8001
```

Zoom管理画面でWebhookエンドポイントを `https://{サーバーIP}:8001/webhook/zoom-zra` に設定し、`zra.conversation_completed` イベントを有効にしてください。

---

## systemd による定期実行（Linux サーバー）

```bash
# ファイルをコピー
sudo cp zoom-to-rr-sync.service /etc/systemd/system/
sudo cp zoom-to-rr-sync.timer /etc/systemd/system/

# デーモン再読み込み・有効化・起動
sudo systemctl daemon-reload
sudo systemctl enable zoom-to-rr-sync.timer
sudo systemctl start zoom-to-rr-sync.timer

# 状態確認
sudo systemctl status zoom-to-rr-sync.timer
```

---

## 使用ライブラリ

| ライブラリ | 用途 |
|---|---|
| `requests` | Zoom API・楽楽販売APIのHTTPリクエスト |
| `fastapi` | WebhookモードのHTTPサーバー（オプション） |
| `uvicorn` | FastAPI用ASGIサーバー（オプション） |
| `python-dotenv` | ローカル開発用の `.env` 読み込み（任意） |

---

## 注意事項

- ZRA APIには `revenue_accelerator:read:admin` スコープが必要です
- ZRA APIのレスポンス構造は実際のAPIレスポンスを見て要調整箇所（`★ 要調整`コメント）を修正してください
- 楽楽販売の各フィールドIDは管理画面のAPI設定から確認してください
