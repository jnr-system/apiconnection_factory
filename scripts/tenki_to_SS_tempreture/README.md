# tenki_to_SS_tempreture

## 概要

気象庁のWebサイトから前日の**全都道府県（47都道府県）の最低気温**を自動スクレイピングし、Googleスプレッドシートの月別シート（日付列）へ書き込むスクリプトです。

---

## ファイル構成

| ファイル | 説明 |
|---|---|
| `tenki_to_SS_tempreture.py` | メインスクリプト |
| `tenki-temperature.service` | systemd サービスユニットファイル |
| `tenki-temperature.timer` | systemd タイマーユニットファイル（定期実行設定） |
| `execution_log.txt` | 実行ログ（自動生成・追記） |

---

## 処理の流れ

```
前日の日付を計算
   ↓ ターゲット年月日・シート名（例: "1月_new"）・書き込み列を決定
気象庁Webサイトをスクレイピング（47都道府県 × 1リクエスト）
   ↓ pandas.read_html() で月間日別データページのHTMLテーブルを取得
   ↓ 0列目（日付）・8列目（最低気温）を抽出してターゲット日の値を取得
   ↓ 都道府県ごとに 0.1秒スリープ（サーバー負荷軽減）
Googleスプレッドシートへ書き込み
   ↓ 47行分のデータを縦一列で一括書き込み
   ↓ 書き込み列に右寄せ書式を適用
```

---

## 詳細な処理内容

### 1. 書き込み位置の計算

```python
BASE_CELL = "B3"  # 北海道・1日のセル位置
base_row, base_col = a1_to_rowcol(BASE_CELL)
target_col = base_col + (target_day - 1)  # N日は基準列+N-1列目
```

- シート名: `{月}月_new` 形式（例: 1月なら `1月_new`）
- 列は `BASE_CELL` から (前日の日付 - 1) 列ずらす
- シートが存在しない場合は自動で新規作成（100行 × 40列）

### 2. スクレイピング (`scrape_jma_target_day()`)

**スクレイピング元**: 気象庁 過去の気象データ検索（月間日別データページ）

```
https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php
  ?prec_no={都道府県コード}&block_no={観測所コード}&year={年}&month={月}&day=&view=
```

- `pandas.read_html()` でHTMLテーブルを取得
- 取得テーブルの `[0列目, 8列目]` を使用:
  - 0列目: 日付（1〜31の数値）
  - 8列目: 最低気温（℃）
- ターゲット日の行を文字列マッチングで検索
- データなし・エラーの場合は `"-"` を返す

**データクリーニング**:
- `nan` → `"-"` に変換
- 余分な記号（`'`, `]`, `)`）を除去

### 3. 都道府県リスト (`ALL_CITIES`)

47都道府県の代表観測地点コードを設定済みです：

| 都道府県 | prec_no | block_no |
|---|---|---|
| 北海道 | 14 | 47412 |
| 青森県 | 31 | 47575 |
| 岩手県 | 33 | 47584 |
| ... | ... | ... |
| 沖縄県 | 91 | 47936 |

各コードは気象庁の観測地点コード体系に対応しています。

### 4. スプレッドシート書き込み

```python
# 縦一列形式: [[北海道の値], [青森県の値], ..., [沖縄県の値]]
sheet.update(range_name=target_cell_a1, values=col_data)

# 書き込み列に右寄せ書式を適用
sheet.format(range_string, {"horizontalAlignment": "RIGHT"})
```

---

## 設定値（スクリプト内）

| 変数 | 値 | 内容 |
|---|---|---|
| `SPREADSHEET_KEY` | ※スクリプト内で設定 | 書き込み先スプレッドシートID |
| `BASE_CELL` | `B3` | 北海道1日分の基準セル（書き込み起点） |
| `ALL_CITIES` | 47都道府県分 | 気象庁観測地点コード（prec_no・block_no）リスト |

---

## 環境変数（必須）

| 変数名 | 説明 |
|---|---|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCPサービスアカウントのJSON文字列 |

> ローカル実行時は同フォルダの `.env` ファイルに記述してください。

---

## 実行方法

```bash
python tenki_to_SS_tempreture.py
```

スクレイピングのサーバー負荷軽減のため、都道府県ごとに **0.1秒のスリープ** を入れています（47都道府県で計約5秒）。

---

## systemd による定期実行（Linux サーバー）

毎日8時に自動実行されます。

```bash
# ファイルをコピー
sudo cp tenki-temperature.service /etc/systemd/system/
sudo cp tenki-temperature.timer /etc/systemd/system/

# デーモン再読み込み・有効化・起動
sudo systemctl daemon-reload
sudo systemctl enable tenki-temperature.timer
sudo systemctl start tenki-temperature.timer

# 状態確認
sudo systemctl status tenki-temperature.timer
```

---

## 使用ライブラリ

| ライブラリ | 用途 |
|---|---|
| `pandas` | 気象庁HTMLテーブルのパース（`read_html()`） |
| `gspread` | Google Sheetsへの書き込みと書式設定 |
| `oauth2client` | GCPサービスアカウント認証 |
| `python-dotenv` | ローカル開発用の `.env` 読み込み（任意） |
