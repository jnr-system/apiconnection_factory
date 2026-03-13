"""
【実行内容】
楽楽販売APIから問い合わせデータ（エディオン等）をCSV形式で取得し、
Googleスプレッドシートに全データを上書き（洗い替え）します。
"""
import requests
import csv
import io
import os
import json
from datetime import datetime
from pathlib import Path
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==============================================================================
# 設定エリア
# ==============================================================================

# ■ 楽楽販売の設定
RAKURAKU_DOMAIN = "hntobias.rakurakuhanbai.jp"
RAKURAKU_TOKEN = os.environ.get("RAKURAKU_TOKEN", "")

RAKURAKU_SCHEMA_ID = "101181"
RAKURAKU_SEARCH_ID = "107497"
RAKURAKU_LIST_ID   = "101473"

# ■ Googleスプレッドシートの設定
SPREADSHEET_KEY = "1hBE66-67Se9c4FHXoILr7s5L-HDj5qjaqp4A6Z44kQ8"
SHEET_NAME = "テスト" # FIXME: 実際のシート名に合わせて変更してください

# ■ ログファイルの保存先
LOG_FILE_PATH = Path(__file__).parent / "execution_log.txt"

# ==============================================================================
# 共通関数: ログ出力
# ==============================================================================
def write_log(message):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{now_str}] {message}"
    print(log_msg)
    try:
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")
    except Exception as e:
        print(f"ログ書き込みエラー: {e}")

# ==============================================================================
# 処理開始
# ==============================================================================
def main():
    write_log("=== 問い合わせデータ連動処理を開始します ===")

    if not RAKURAKU_TOKEN:
        write_log("エラー: 環境変数 RAKURAKU_TOKEN が設定されていません。")
        return

    # 1. 楽楽販売からCSVデータを取得
    # URLにはアカウント名 mspy4wa が入る
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN
    }
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "listId": RAKURAKU_LIST_ID,    
        "searchId": RAKURAKU_SEARCH_ID,
        "limit": 5000 
    }

    write_log("楽楽販売からデータをダウンロード中...")
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=60)
        
        if res.status_code != 200:
            write_log(f"エラー: 楽楽販売APIが失敗しました ({res.status_code})")
            return

        # cp932 でデコードを試みる
        try:
            csv_content = res.content.decode("cp932")
        except UnicodeDecodeError:
            csv_content = res.content.decode("utf-8", errors="ignore")

        f_obj = io.StringIO(csv_content)
        reader = csv.reader(f_obj)
        raw_data = list(reader)

        if not raw_data:
            write_log("データが空でした。処理を終了します。")
            return
            
        write_log(f"取得成功: {len(raw_data)} 行のデータを取得しました。")

        # -------------------------------------------------------------
        # 全データお掃除処理
        # -------------------------------------------------------------
        data_list = []
        for row in raw_data:
            new_row = []
            for cell in row:
                # 全てのセルに対して、前後の空白除去 ＆ 先頭の ' を削除
                clean_cell = cell.strip().lstrip("'")
                new_row.append(clean_cell)
            data_list.append(new_row)
            
        write_log("データの整形完了 (全てのクォートを削除)")

    except Exception as e:
        write_log(f"楽楽販売へのアクセス中にエラーが発生: {e}")
        return

    # 2. Googleスプレッドシートを更新
    write_log("Googleスプレッドシートに接続中...")
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not json_str:
            write_log("エラー: 環境変数 GOOGLE_SERVICE_ACCOUNT_JSON が設定されていません。")
            return

        creds_dict = json.loads(json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        workbook = client.open_by_key(SPREADSHEET_KEY)
        
        # シートが見つからない場合は最初のシートを使う
        try:
            worksheet = workbook.worksheet(SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            write_log(f"シート '{SHEET_NAME}' が見つかりませんでした。最初のシートを使用します。")
            worksheet = workbook.get_worksheet(0)

        worksheet.clear()

        write_log(f"スプレッドシート '{worksheet.title}' を更新しています...")
        
        worksheet.update(
            range_name='A1', 
            values=data_list, 
            value_input_option='USER_ENTERED'
        )

        write_log("=== 完了: スプレッドシートの更新が終わりました ===")

    except Exception as e:
        write_log(f"スプレッドシートの更新エラー: {e}")

if __name__ == "__main__":
    main()
