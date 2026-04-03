# RR_to_RMS_Lstage-customerdata

## 概要

楽楽販売に登録された「待機中の注文」に対して、楽天RMS（受注管理システム）から対応する注文情報を取得し、顧客情報・配送先情報・商品明細・ガス種・進捗を楽楽販売のレコードへ自動転記するスクリプトです。

---

## 処理の流れ

```
楽楽販売（CSVエクスポートAPI）
   ↓ 「待機中」ステータスのレコードを最大1,000件取得
   ↓ 郵便番号が空（未更新）のレコードのみ対象
   ↓ 楽天注文番号 → 楽楽キーID のマップを作成
楽天RMS API（注文検索）
   ↓ 直近3日分の注文番号を最大4ページ（最大4,000件）検索
楽天RMS API（注文詳細取得）
   ↓ 注文番号を100件ずつに分割してバッチ取得（version=7）
楽楽販売（レコード更新API）
   ↓ [1回目] 顧客情報・配送先・金額・配送希望日・ガス種・進捗を書き込み
   ↓ [2回目] 商品明細（商品キー・個数）を追加（getSubordinate=1）
完了ログ出力
```

---

## 詳細な処理内容

### 1. 楽楽販売から待機データを取得 (`get_rakuraku_targets()`)

- CSVエクスポートAPI (`/api/csvexport/version/v1`) を使用
- 対象DB: `dbSchemaId=101357`、絞り込み設定: `searchId=105786`
- 最大1,000件を取得し、**配送先郵便番号（39列目）が空のレコードのみ**対象とする
  - 郵便番号が入っている = 更新済みとして除外
- 1列目（楽楽キーID）と2列目（楽天注文番号）から `{注文番号: キーID}` の辞書を作成
- レスポンスは CP932 でデコード（失敗時は ignore オプションで継続）

### 2. 楽天RMS APIから注文情報を取得 (`get_rms_orders()`)

**認証**
- サービスシークレットとライセンスキーを `Base64(secret:key)` 形式でエンコードし、`Authorization: ESA {token}` ヘッダーで送信

**注文検索 (searchOrder)**
- 検索期間: 現在時刻から3日前まで（JST）
- 1ページあたり最大1,000件、最大4ページまで取得（上限設定）
- ページにデータがなくなれば早期終了

**注文詳細取得 (getOrder)**
- 収集した注文番号を100件ずつに分割してバッチリクエスト
- `version: "7"` で `merchantDefinedSkuId`・`skuInfo` を含む詳細データを取得

### 3. 楽楽販売レコードの更新 (`main()`)

RMS注文番号と楽楽販売のマップが一致した場合、1件につき2回APIを呼び出します。

**1回目: ヘッダ更新（`getSubordinate` なし）**

| フィールドID | 内容 | RMSデータソース |
|---|---|---|
| `113811` | 注文者の電話番号 | `OrdererModel.phoneNumber1-2-3` |
| `113772` | 注文合計金額 | `totalPrice` |
| `113095` | 配送先郵便番号 | `SenderModel.zipCode1+zipCode2` |
| `113073` | 配送希望日 | `remarks` フィールドを正規表現 `\d{4}-\d{2}-\d{2}` でパース |
| `113096` | 配送先都道府県 | `SenderModel.prefecture` |
| `113097` | 配送先市区町村 | `SenderModel.city` |
| `113098` | 配送先番地・建物名 | `SenderModel.subAddress` |
| `113089` | 配送先電話番号 | `SenderModel.phoneNumber1-2-3` |
| `113051` | ガスの種類 | `skuInfo` から判定（都市ガス / LPガス / なし） |
| `113100` | 進捗 | `merchantDefinedSkuId` から判定（下記参照） |

**進捗の判定ロジック**

| `merchantDefinedSkuId` に含まれるワード | 進捗の値 |
|---|---|
| `kouzi` | 工事込案件 |
| `zaiko` かつ `楽天` | 楽天倉庫出荷 |
| `zaiko`（`楽天` なし） | 自社在庫出荷 |
| `楽天倉庫在庫` | 楽天倉庫在庫 |
| いずれも含まない | （空） |

**2回目: 商品明細追加（`getSubordinate=1`）**

- `merchantDefinedSkuId` から9桁の数字を正規表現で抽出し、商品キーとして登録
- 商品キー1つにつき1明細行を追加

| フィールドID | 内容 |
|---|---|
| `113053` | 商品選択（DBリンク） |
| `113058` | 個数 |

連続アクセス防止のため、各APIコール後に **1秒のスリープ** を入れています。

---

## ファイル構成

| ファイル | 説明 |
|---|---|
| `RR_to_RMS_Lstage-customerdata.py` | メインスクリプト |
| `rr-customerdata.service` | systemd サービスユニットファイル |
| `rr-customerdata.timer` | systemd タイマーユニットファイル（定期実行設定） |
| `execution_log.txt` | 実行ログ（自動生成・追記） |
| `.env` | ローカル開発用の環境変数ファイル（Git管理外） |

---

## 環境変数（必須）

| 変数名 | 説明 |
|---|---|
| `RAKURAKU_TOKEN` | 楽楽販売 APIトークン |
| `RMS_SERVICE_SECRET` | 楽天RMS サービスシークレット |
| `RMS_LICENSE_KEY` | 楽天RMS ライセンスキー |

> ローカル実行時は同フォルダの `.env` ファイルに記述（`python-dotenv` が自動読み込み）。
> 本番環境では GitHub Actions Secrets または OS の環境変数として設定してください。

---

## 実行方法

```bash
python RR_to_RMS_Lstage-customerdata.py
```

ログは同フォルダの `execution_log.txt` に追記されます。

---

## systemd による定期実行（Linux サーバー）

```bash
# サービスとタイマーを有効化
sudo systemctl enable rr-customerdata.timer
sudo systemctl start rr-customerdata.timer

# 状態確認
sudo systemctl status rr-customerdata.timer
```

---

## 使用ライブラリ

| ライブラリ | 用途 |
|---|---|
| `requests` | 楽楽販売API・楽天RMS APIのHTTPリクエスト |
| `base64` | RMS認証トークンのエンコード |
| `pytz` | JST（日本標準時）での日時計算 |
| `re` | 配送希望日・商品キーの正規表現パース |
| `csv`, `io` | CSVレスポンスのパース |
| `python-dotenv` | ローカル開発用の `.env` 読み込み（任意） |
