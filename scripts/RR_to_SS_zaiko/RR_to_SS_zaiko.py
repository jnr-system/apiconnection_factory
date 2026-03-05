"""
【実行内容】
楽楽販売APIから在庫一覧データをCSV形式で取得し、Googleスプレッドシートの「楽楽販売在庫表」シートに
全データを上書き（洗い替え）します。データ整形（クォート削除等）も行います。
"""
import requests
import csv
import io
import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
from datetime import datetime
import os
import json

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 本番環境では python-dotenv がなくても動作する

# ==============================================================================
# 設定エリア
# ==============================================================================

# ■ 楽楽販売の設定
RAKURAKU_DOMAIN = "hntobias.rakurakuhanbai.jp"
RAKURAKU_TOKEN = os.environ["RAKURAKU_TOKEN"]

RAKURAKU_SCHEMA_ID = "101296"
RAKURAKU_SEARCH_ID = "104283"
RAKURAKU_LIST_ID   = "101100"

# ■ Googleスプレッドシートの設定
SPREADSHEET_KEY = "1kfTsCQPKGSFsSPIvuPJ4Yi9SAzXC_hfT5FHWCNm5IuU"
SHEET_NAME = "楽楽販売在庫表"

# ■ ログファイルの保存先
LOG_FILE_PATH = Path(__file__).parent / "inventory_log.txt"

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
    write_log("=== 在庫連動処理を開始します ===")

    # 1. 楽楽販売からCSVデータを取得
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
                # これで日付も数字もすべて「生のデータ」になります
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
        creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        workbook = client.open_by_key(SPREADSHEET_KEY)
        worksheet = workbook.worksheet(SHEET_NAME)

        worksheet.clear()

        write_log("スプレッドシートを更新しています...")
        
        # USER_ENTERED (自動判定) で貼り付け
        # ・日付文字列 → 日付データに変換
        # ・数字文字列 → 数字データに変換 (001 は 1 になる)
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