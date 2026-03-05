# 給湯器設置写真 管理 API

Google Cloud Storage (GCS) と楽楽販売を連携した FastAPI 製の画像アップロード・閲覧システムです。  
問い合わせフォーム送信時に設置写真を GCS へアップロードし、楽楽販売のレコードに閲覧URLを自動書き込みします。

---

## セットアップ

### 1. 依存パッケージのインストール

venv を使う場合（プロジェクトルートの venv を共有）:

```bash
cd /root/project/apiconnection_factory
source venv/bin/activate
pip install -r scripts/otoiawase_photo_upload/requirements.txt
```

### 2. 環境変数の設定

他のスクリプトと同様に、機密情報はすべて環境変数で管理します。  
`/etc/photo-api.env` ファイルを作成してください（systemd の EnvironmentFile として使用）:

```ini
# /etc/photo-api.env

# 必須: 楽楽販売 API トークン（他スクリプトと共通）
RAKURAKU_TOKEN=your-api-token-here

# 必須: GCS サービスアカウント JSON（他スクリプトと共通）
# google_secret.json の内容を1行のJSON文字列として貼り付け
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"..."}

# 必須: このサーバーのベース URL（楽楽販売に書き込む閲覧URLのベース）
SERVER_BASE_URL=http://YOUR_CONOHA_IP_OR_DOMAIN:8000

# 任意: デフォルト値から変更したい場合のみ設定
# GCS_BUCKET_NAME=rakuraku-photos_factory-jnr
# RAKURAKU_DOMAIN=hntobias.rakurakuhanbai.jp
# RAKURAKU_ACCOUNT=mspy4wa
# RAKURAKU_INQUIRY_DB_ID=101443
# RAKURAKU_INQUIRY_SEARCH_ID=107323
# RAKURAKU_INQUIRY_LIST_ID=101471
# RAKURAKU_URL_FIELD_ID=115927
```

> **セキュリティ注意**: `/etc/photo-api.env` のパーミッションを制限してください。
> ```bash
> sudo chmod 600 /etc/photo-api.env
> ```

---

## ConoHa VPS へのデプロイ（systemd）

### 1. サービスファイルの配置

```bash
sudo cp photo-api.service /etc/systemd/system/photo-api.service
sudo systemctl daemon-reload
sudo systemctl enable photo-api
sudo systemctl start photo-api
```

### 2. ファイアウォール設定

```bash
ufw allow 8000
```

### 3. 動作確認

```bash
sudo systemctl status photo-api
sudo journalctl -u photo-api -f
```

### 4. サービスファイルの WorkingDirectory を確認

`photo-api.service` の以下を環境に合わせて変更してください:

```ini
WorkingDirectory=/root/project/apiconnection_factory/scripts/otoiawase_photo_upload
ExecStart=/root/project/apiconnection_factory/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
```

---

## エンドポイント一覧

| メソッド | パス | 説明 |
|---|---|---|
| GET | `/contact` | お問い合わせフォーム（テスト用・正直屋フォーム再現版） |
| POST | `/contact/submit` | フォーム送信処理（GCSアップロード + 楽楽販売更新） |
| GET | `/form` | シンプルな画像アップロードフォーム |
| POST | `/upload/{inquiry_id}` | GCSへ画像をアップロード（電話番号指定で楽楽販売連携） |
| GET | `/view/{folder}` | GCS上の画像を署名付きURLで閲覧（HTML） |
| GET | `/debug/fields` | 楽楽販売CSVのヘッダー確認用 |
| GET | `/debug/record/{key_id}` | 楽楽販売レコードのJSON取得確認用 |

起動後 → `http://YOUR_SERVER:8000/docs` で Swagger UI が使えます。

---

## 楽楽販売連携の流れ

1. 電話番号（ハイフン除去後）で楽楽販売のCSVを検索
2. 一致するレコードの **キーID** を取得
3. `keyId` を使ってレコードの **「画像用URL」フィールドID: `115927`** に閲覧URLを書き込み

## GCS フォルダ構成

```
(バケット名)/
└── photos/
    ├── 山田太郎/          ← 顧客名（楽楽販売から取得 or フォームの名前）
    │   ├── photo1.jpg
    │   └── photo2.jpg
    └── 09012345678/       ← 名前が空の場合は電話番号（ハイフンなし）
```

---

## Laravel との統合（パターン①推奨）

```php
// ContactController.php
if ($request->hasFile('photos')) {
    $client = new \GuzzleHttp\Client();
    $multipart = [
        ['name' => 'name',  'contents' => $request->name],
        ['name' => 'phone', 'contents' => $request->phone],
        ['name' => 'email', 'contents' => $request->email],
    ];
    foreach ($request->file('photos') as $file) {
        $multipart[] = [
            'name'     => 'files',
            'contents' => fopen($file->getRealPath(), 'r'),
            'filename' => $file->getClientOriginalName(),
        ];
    }
    $client->post('https://YOUR_VPS/contact/submit', ['multipart' => $multipart]);
}
```
