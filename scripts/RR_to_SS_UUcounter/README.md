# RR_to_SS_UUcounter

## 概要

楽楽販売の問い合わせDBからデータを取得し、**Gemini AI（Google）** を使って各問い合わせの商材カテゴリ・都道府県を自動判定します。判定結果をGoogleスプレッドシートの日別シート（UU件数マトリクス）と全体シート（月別詳細集計）に書き込むスクリプトです。

---

## ファイル構成

| ファイル | 説明 |
|---|---|
| `RR_to_SS_UUcounter-master.py` | メインスクリプト（UU件数集計） |
| `RR_to_SS_UUcounter-sonotanodenwa.py` | 「その他の電話」専用の別集計バージョン |
| `execution_log.txt` | 実行ログ（自動生成・追記） |

---

## 処理の流れ

```
楽楽販売（CSVエクスポートAPI）
   ↓ 前日の問い合わせデータを最大10,000件取得（dbSchemaId: 101181）
データ整形
   ↓ 登録日でフィルタリング、都道府県列・住所列から都道府県を初期判定
Gemini API（AI分類）
   ↓ 住所テキストから都道府県を判定（20件ずつバッチ送信、JSON形式で返答）
商品タイプ判定（Python）
   ↓ 「新規商品タイプ」列のキーワードで給湯器/エコキュートを判定
ステータス除外チェック（Python）
   ↓ 重複・クレーム・エリア外などのステータスは集計対象外に
集計
   ↓ 商材 × 都道府県 × 日別で UU件数をカウント
Googleスプレッドシート更新
   ↓ 日別シートと全体シートを更新
```

---

## 詳細な処理内容

### 1. 楽楽販売からのデータ取得 (`fetch_rakuraku_csv()`)

- DB: 問い合わせ管理（`dbSchemaId: 101181`, `listId: 101446`, `searchId: 107213`）
- 最大10,000件を取得、レスポンスは CP932 でデコード（失敗時は UTF-8 で再試行）
- pandas の `read_csv()` でDataFrame化

### 2. データ整形と前処理 (`load_and_clean_data()`)

- `登録日` を datetime に変換し、前日の日付範囲でフィルタリング
- 都道府県の初期判定 (`get_prefecture_simple()`):
  1. `都道府県名` 列に値があればそれを使用
  2. なければ `メール差し込み用：住所` の先頭が都道府県名と一致するか確認
  3. 一致しなければ「不明」
- 同一記録IDのレコードをグルーピングし、最初の行だけ残す（重複除去）

### 3. Gemini AIによる都道府県特定 (`classify_all_with_gemini()`)

- 使用モデル: `gemini-3-flash-preview`
- 20件ずつバッチでGemini APIに送信し、JSON形式で都道府県名を返させる
- System Prompt の内容:
  - 住所テキストから都道府県を特定する
  - 特定できない場合は「不明」とする
- APIエラー時は最大3回リトライ（2秒待機）

### 4. 商品タイプ判定と除外処理

**商材カテゴリ（Python側で判定）**

| 判定 | 条件 |
|---|---|
| 給湯器 | `新規商品タイプ` 列に「給湯器」を含む |
| エコキュート | `新規商品タイプ` 列に「エコキュート」または「電気温水器」を含む |
| 不明 | 上記以外（集計対象外） |

**除外ステータス（対象外扱い）**

以下のキーワードが `対応状況ステータス` に含まれる場合は除外:
- 重複 / エリア外、施工・対応不可 / クレーム / 連絡禁止 / 修理対応依頼 / その他（問い合わせ以外の物）

### 5. スプレッドシート更新

**日別シート（例: 「1月」シート）**

- 書き込み対象: UU件数のみ
- 給湯器列起点: C列（`COL_START_GAS = 3`）、エコキュート列起点: AI列（`COL_START_ECO = 35`）
- 行構成: 6行目〜 都道府県順（北海道〜沖縄）、53行目: 不明
- 日付 N 日は、起点列から (N-1) 列ずれた列に書き込み
- 件数が0の場合は `"-"` を書き込み

**全体シート**

- 商材×問い合わせ種別ごとの月別件数を加算更新（上書きではなく既存値に加算）
- 対象行:
  - 給湯器（UU）: 6行目、給湯器（修理）: 7行目、給湯器（止）: 8行目、給湯器（その他電話）: 9行目
  - エコキュート（UU）: 10行目〜13行目
  - コンロ（UU）: 14行目、その他商品（UU）: 15行目
- 対象列: 1月=C列、2月=D列…（`target_col = target_month + 2`）

---

## 環境変数（必須）

| 変数名 | 説明 |
|---|---|
| `RAKURAKU_TOKEN` | 楽楽販売 APIトークン |
| `GEMINI_API_KEY` | Google Gemini APIキー |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCPサービスアカウントのJSON文字列 |

---

## 設定値（スクリプト内）

| 変数 | 内容 |
|---|---|
| `SPREADSHEET_KEY` | 書き込み先スプレッドシートID |
| `GEMINI_MODEL_NAME` | 使用するGeminiモデル名（`gemini-3-flash-preview`） |
| `DB_INQUIRY` | 楽楽販売のDB・検索・一覧ID設定 |
| `COL_START_GAS` | 給湯器の1日目の列番号（3=C列） |
| `COL_START_ECO` | エコキュートの1日目の列番号（35=AI列） |
| `START_ROW` | 都道府県データ開始行（6行目） |
| `UNKNOWN_ROW` | 「不明」都道府県の行（53行目） |

---

## 実行方法

```bash
# メインスクリプト（UUカウント）
python RR_to_SS_UUcounter-master.py

# その他の電話専用バージョン
python RR_to_SS_UUcounter-sonotanodenwa.py
```

---

## systemd による定期実行（Linux サーバー）

毎日8時に自動実行されます。

```bash
# ファイルをコピー
sudo cp rr-uucounter.service /etc/systemd/system/
sudo cp rr-uucounter.timer /etc/systemd/system/

# デーモン再読み込み・有効化・起動
sudo systemctl daemon-reload
sudo systemctl enable rr-uucounter.timer
sudo systemctl start rr-uucounter.timer

# 状態確認
sudo systemctl status rr-uucounter.timer
```

---

## 使用ライブラリ

| ライブラリ | 用途 |
|---|---|
| `pandas` | DataFrameでのデータ整形・集計 |
| `requests` | 楽楽販売APIのHTTPリクエスト |
| `google-genai` | Gemini APIクライアント |
| `gspread` | Google Sheetsへの書き込み |
| `oauth2client` | GCPサービスアカウント認証 |
| `python-dotenv` | ローカル開発用の `.env` 読み込み（任意） |
