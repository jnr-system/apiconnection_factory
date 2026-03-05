# RR_to_SS_zaiko

## 概要

楽楽販売の在庫管理DBから全在庫データをCSVで取得し、Googleスプレッドシートの「楽楽販売在庫表」シートへ**全件上書き（洗い替え）**するスクリプトです。

---

## 処理の流れ

```
楽楽販売（CSVエクスポート）
   ↓ 在庫管理DBの全データを最大5,000件取得
データ整形
   ↓ 全セルの前後空白・先頭シングルクォートを除去してクリーニング
Googleスプレッドシート更新
   ↓ 対象シートをクリア → A1から全データを一括書き込み（USER_ENTERED形式）
完了ログ
```

---

## 詳細な処理内容

### データ取得
- 楽楽販売 DB（dbSchemaId: 101296）からCSV形式でエクスポート
- デコードは UTF-8（取得できない場合は ignore）

### データ整形
- 全セルの `strip()` および `lstrip("'")` を実施
- 日付・数値が文字列として格納されている場合も `USER_ENTERED` モードで貼ることでスプレッドシートが自動変換

### スプレッドシート書き込み
- 既存データを `clear()` でリセット後、`A1` セルから全データを貼り付け

---

## 設定値（スクリプト内）

| 変数 | 内容 |
|---|---|
| `RAKURAKU_SCHEMA_ID` | 楽楽販売 在庫DB Schema ID |
| `RAKURAKU_SEARCH_ID` | 絞り込み設定ID |
| `RAKURAKU_LIST_ID` | 一覧画面設定ID |
| `SPREADSHEET_KEY` | 書き込み先スプレッドシートID |
| `SHEET_NAME` | 書き込み先シート名（`楽楽販売在庫表`） |

---

## 環境変数（必須）

| 変数名 | 説明 |
|---|---|
| `RAKURAKU_TOKEN` | 楽楽販売 APIトークン |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCPサービスアカウントのJSON文字列 |

---

## 実行方法

```bash
python RR_to_SS_zaiko.py
```

ログは同フォルダの `inventory_log.txt` に追記されます。
