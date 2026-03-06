# apiconnection_factory

楽楽販売・楽天RMS・Googleスプレッドシート・気象庁などの外部サービスとAPI連携するPythonスクリプトの格納リポジトリです。

---

## スクリプト一覧

| フォルダ | 概要 |
|---|---|
| [RR_to_RMS_customerdata](./scripts/RR_to_RMS_customerdata/) | 楽楽販売の待機注文に対し楽天RMSから顧客情報を取得→楽楽販売へ自動転記 |
| [RR_to_SS_UUcounter](./scripts/RR_to_SS_UUcounter/) | 楽楽販売の問い合わせをGemini AIで自動分類 → スプレッドシートへUU数を集計 |
| [RR_to_SS_seiyakuprocess](./scripts/RR_to_SS_seiyakuprocess/) | 楽楽販売の問い合わせから緊急度・商品タイプ別のKPI（UU/成約/終了）を集計 |
| [RR_to_SS_uriage-arari](./scripts/RR_to_SS_uriage-arari/) | 楽楽販売の売上・粗利データをDBごと・商品別に集計 → スプレッドシートへ書き込み |
| [RR_to_SS_zaiko](./scripts/RR_to_SS_zaiko/) | 楽楽販売の在庫データをスプレッドシートへ全件洗い替え |
| [RR_to_SS_zenkokuseiyaku](./scripts/RR_to_SS_zenkokuseiyaku/) | 楽楽販売の成約データを商品タイプ・都道府県別に集計 → スプレッドシートへ書き込み |
| [otoiawase_photo_upload](./scripts/otoiawase_photo_upload/) | 問い合わせ時の設置写真をGCSへアップロードし楽楽販売の画像用URLを自動更新 |
| [tenki_to_SS_tempreture](./scripts/tenki_to_SS_tempreture/) | 気象庁から全都道府県の最低気温をスクレイピング → スプレッドシートへ書き込み |

---

## 共通の前提

- 各スクリプトは **環境変数** で認証情報を管理しています（APIキーをコードに直書きしないこと）
- Googleスプレッドシートへの書き込みには `GOOGLE_SERVICE_ACCOUNT_JSON` が必要です
- 楽楽販売との連携には `RAKURAKU_TOKEN` が必要です

### 主な環境変数

| 変数名 | 説明 |
|---|---|
| `RAKURAKU_TOKEN` | 楽楽販売 APIトークン |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCPサービスアカウントのJSON文字列 |
| `GEMINI_API_KEY` | Google Gemini APIキー（UUcounterのみ） |
| `RMS_SERVICE_SECRET` | 楽天RMS サービスシークレット（RMS連携のみ） |
| `RMS_LICENSE_KEY` | 楽天RMS ライセンスキー（RMS連携のみ） |

---

## 依存パッケージ

```bash
pip install -r requirements.txt
```
