# フォーム問い合わせデータ連携 (エディオン等)

楽楽販売APIからフォーム経由などで入力された問い合わせデータをCSV形式で取得し、Googleスプレッドシートに洗い替えで出力します。

## 対象DB
- 問い合わせ管理DB (Schema ID: `101181`, Search ID: `107497`, List ID: `101473`)

## 環境変数
ローカル実行用に、このディレクトリ内に `.env` ファイルを作成し、以下を記述してください。

```
RAKURAKU_TOKEN=your_token_here
GOOGLE_SERVICE_ACCOUNT_JSON={"type": "service_account", ...}
```

※ 本番環境ではGitHub Actions Secretsまたは環境変数で同じものを渡す想定です。

## 実行方法

```bash
cd apiconnection_factory
python scripts/RR_to_SS_edion/RR_to_SS_edion.py
```

## ログ
実行ログは、同じディレクトリ内の `execution_log.txt` に追記されます。
