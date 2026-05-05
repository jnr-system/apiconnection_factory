# RR_to_RMS_Lstage-customerdata

## 概要

楽楽販売（L業務管理DB）と楽天RMS間の受注・出荷情報を双方向に同期する3本のスクリプト群です。

| スクリプト | 方向 | 概要 |
|---|---|---|
| `RR_to_RMS_Lstage-customerdata.py` | RMS → 楽楽販売 | 待機中注文に対しRMSから顧客情報・配送先・商品明細を取得して楽楽販売へ転記 |
| `RMS_to_RR_Lstage-shippingday-reflect.py` | RMS → 楽楽販売 | RMSに登録された出荷日を楽楽販売へ反映し、進捗を「納期回答あり」に更新 |
| `RR_to_RMS_Lstage-shippingday-kouzi.py` | 楽楽販売 → RMS | 楽楽販売の「工事手配完了」レコードのメーカー出荷日をRMSへ出荷確定として登録 |

---

## スクリプト詳細

---

### 1. RR_to_RMS_Lstage-customerdata.py（30分毎）

#### 処理の流れ

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

#### 詳細な処理内容

**楽楽販売から待機データを取得**
- 対象DB: `dbSchemaId=101357`、絞り込み設定: `searchId=105786`
- 最大1,000件を取得し、**配送先郵便番号（39列目）が空のレコードのみ**対象とする

**楽天RMS APIから注文情報を取得**
- 検索期間: 現在時刻から3日前まで（JST）
- 1ページあたり最大1,000件、最大4ページ（上限設定）
- `version: "7"` で `merchantDefinedSkuId`・`skuInfo` を含む詳細データを取得

**楽楽販売レコードの更新（1件につき2回APIコール）**

| フィールドID | 内容 |
|---|---|
| `113811` | 注文者電話番号 |
| `113772` | 注文合計金額 |
| `113095` | 配送先郵便番号 |
| `113073` | 配送希望日（remarks から正規表現でパース） |
| `113096` | 配送先都道府県 |
| `113097` | 配送先市区町村 |
| `113098` | 配送先番地・建物名 |
| `113089` | 配送先電話番号 |
| `113051` | ガスの種類（都市ガス / LPガス / なし） |
| `113100` | 進捗（`merchantDefinedSkuId` から判定） |
| `113053` | 商品選択（2回目: DBリンク） |
| `113058` | 個数（2回目） |

**進捗の判定ロジック**

| `merchantDefinedSkuId` に含まれるワード | 進捗の値 |
|---|---|
| `kouzi` | 工事込案件 |
| `zaiko` かつ `楽天` | 楽天倉庫出荷 |
| `zaiko`（`楽天` なし） | 自社在庫出荷 |
| `楽天倉庫在庫` | 楽天倉庫在庫 |
| いずれも含まない | （空） |

---

### 2. RMS_to_RR_Lstage-shippingday-reflect.py（毎日9時）

#### 処理の流れ

```
楽楽販売（CSVエクスポートAPI）
   ↓ 進捗「納期回答待ち」のレコードを最大1,000件取得（searchId: 105790）
楽天RMS API（注文検索 + 注文詳細取得）
   ↓ 過去30日分の注文を最大10ページ取得
   ↓ 100件ずつバッチで注文詳細（出荷日）を取得
楽楽販売（レコード更新API）
   ↓ 楽天注文番号で照合し一致したレコードの出荷日（113102）を書き込み
   ↓ 進捗（113100）を「納期回答あり」に更新
完了ログ出力（execution_log_shippingday_reflect.txt）
```

#### 特徴
- `--dry-run` オプション付きで実行すると楽楽販売への書き込みを行わず結果のみ表示
- RMSに出荷日が未登録の注文はスキップ

---

### 3. RR_to_RMS_Lstage-shippingday-kouzi.py（毎日9時）

#### 処理の流れ

```
楽楽販売（CSVエクスポートAPI）
   ↓ 進捗「工事手配完了」のレコードを最大100件取得（searchId: 105858）
   ↓ 「注文ID」「楽天受注番号」「メーカー出荷日」列を取得
楽天RMS API（getOrder 一括照会）
   ↓ 出荷日が未登録の注文のみ対象に絞り込み
楽天RMS API（updateOrderShipping）
   ↓ 配送会社「その他」・伝票番号「-」でRMS出荷確定を登録
完了ログ出力（execution_log_kouzi.txt）
```

#### 特徴
- `--dry-run` オプション付きで実行するとRMSへの書き込みを行わず結果のみ表示
- RMSにすでに出荷日が登録済みの注文は重複登録しないようスキップ

---

## ファイル構成

| ファイル | 説明 |
|---|---|
| `RR_to_RMS_Lstage-customerdata.py` | 顧客情報転記スクリプト |
| `rr-customerdata.service` | 顧客情報転記 systemd サービス |
| `rr-customerdata.timer` | 顧客情報転記 systemd タイマー（30分毎） |
| `RMS_to_RR_Lstage-shippingday-reflect.py` | RMS出荷日→楽楽販売反映スクリプト |
| `rms-to-rr-shippingday-reflect.service` | 出荷日反映 systemd サービス |
| `rms-to-rr-shippingday-reflect.timer` | 出荷日反映 systemd タイマー（毎日9時） |
| `RR_to_RMS_Lstage-shippingday-kouzi.py` | 工事手配完了→RMS出荷確定スクリプト |
| `rr-to-rms-shippingday-kouzi.service` | 出荷確定 systemd サービス |
| `rr-to-rms-shippingday-kouzi.timer` | 出荷確定 systemd タイマー（毎日9時） |
| `execution_log.txt` | 顧客情報転記ログ（自動生成） |
| `execution_log_shippingday_reflect.txt` | 出荷日反映ログ（自動生成） |
| `execution_log_kouzi.txt` | 出荷確定ログ（自動生成） |
| `.env` | ローカル開発用環境変数ファイル（Git管理外） |

---

## 環境変数（必須）

| 変数名 | 説明 |
|---|---|
| `RAKURAKU_TOKEN` | 楽楽販売 APIトークン |
| `RMS_SERVICE_SECRET` | 楽天RMS サービスシークレット |
| `RMS_LICENSE_KEY` | 楽天RMS ライセンスキー |

> ローカル実行時は同フォルダの `.env` ファイルに記述（`python-dotenv` が自動読み込み）。
> 本番環境では `/etc/apiconnection.env` に設定してください。

---

## 実行方法

```bash
# 顧客情報転記
python RR_to_RMS_Lstage-customerdata.py

# RMS出荷日→楽楽販売反映（--dry-run で書き込みなし確認可）
python RMS_to_RR_Lstage-shippingday-reflect.py [--dry-run]

# 工事手配完了→RMS出荷確定（--dry-run で書き込みなし確認可）
python RR_to_RMS_Lstage-shippingday-kouzi.py [--dry-run]
```

---

## systemd による定期実行（Linux サーバー）

```bash
# 顧客情報転記（30分毎）
sudo cp rr-customerdata.service /etc/systemd/system/
sudo cp rr-customerdata.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable rr-customerdata.timer
sudo systemctl start rr-customerdata.timer

# 出荷日反映（毎日9時）
sudo cp rms-to-rr-shippingday-reflect.service /etc/systemd/system/
sudo cp rms-to-rr-shippingday-reflect.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable rms-to-rr-shippingday-reflect.timer
sudo systemctl start rms-to-rr-shippingday-reflect.timer

# 出荷確定（毎日9時）
sudo cp rr-to-rms-shippingday-kouzi.service /etc/systemd/system/
sudo cp rr-to-rms-shippingday-kouzi.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable rr-to-rms-shippingday-kouzi.timer
sudo systemctl start rr-to-rms-shippingday-kouzi.timer

# 状態確認
sudo systemctl status rr-customerdata.timer
sudo systemctl status rms-to-rr-shippingday-reflect.timer
sudo systemctl status rr-to-rms-shippingday-kouzi.timer
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
