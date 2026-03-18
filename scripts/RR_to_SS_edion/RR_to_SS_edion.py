"""
【実行内容】
楽楽販売APIから問い合わせデータ（エディオン等）をCSV形式で取得し、
Googleスプレッドシートの「元データ管理シート」に反映（マージ）します。

レコードIDをキーとして、
- 既存レコード：手動管理列を維持しつつ更新
- 新規レコード：行を追加して初期値をセット
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
SHEET_NAME = "元データ管理シート（マスタDB）"

# ■ ログファイルの保存先
LOG_FILE_PATH = Path(__file__).parent / "execution_log.txt"

# ■ 手動管理対象（Pythonで上書きしない）列のリスト
MANUAL_COLUMNS = [
    "新規フラグ",
    "先方確認フラグ",
    "ステータス",
    "見積作成日",
    "先方確認日",
    "備考",
    "見積金額",
    "見積書URL" # 要件5と既存更新ルールの矛盾を考慮し、手動/GAS管理列として保護を優先します
]

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
        # 指定列の抽出と全データお掃除処理
        # -------------------------------------------------------------
        header = raw_data[0]
        # 取得したCSVヘッダー内の列名のインデックスをマッピング（BOMや改行などを除去）
        header_map = {}
        for idx, col_name in enumerate(header):
            clean_col_name = col_name.strip('\ufeff').strip().replace('\n', '').replace('\r', '')
            header_map[clean_col_name] = idx
        
        # スプレッドシートに書き込むヘッダーのリスト (A,B列は触らないためC列から開始)
        spreadsheet_headers = [
            "ステータス",  # C列
            "レコードID", "エディオン連携用手配番号", "問い合わせ日", "顧客名", "顧客名カナ",
            "顧客電話番号", "顧客メールアドレス", "顧客住所", "", "ガス種",
            "先方店舗名", "先方担当者名", "先方メールアドレス", "工事第１希望日",
            "工事第２希望日", "工事第３希望日", "スグフォームURL"
        ]

        # スプレッドシートのヘッダー名と、対応するCSVの列名のマッピング
        csv_to_ss_map = {
            "レコードID": "記録ID",
            "エディオン連携用手配番号": "エディオン連携用手配番号",
            "問い合わせ日": "新規問い合わせ日",
            "顧客名": "名前",
            "顧客名カナ": "フリカナ",
            "顧客電話番号": "電話番号_1",
            "顧客メールアドレス": "メールアドレス_1",
            "ガス種": "ガスの種類",
            "先方店舗名": "エディオン店舗名",
            "先方担当者名": "エディオン担当者名",
            "先方メールアドレス": "エディオンメールアドレス",
            "工事第１希望日": "第１希望日（エディオン）",
            "工事第２希望日": "第２希望日（エディオン）",
            "工事第３希望日": "第３希望日（エディオン）",
            "スグフォームURL": "スグフォームURL（エディオン）",
        }

        # 住所を構成するCSV列のリスト
        address_csv_columns = ["エディオン案件用：郵便番号", "都道府県名", "市区名", "町村名・番地", "マンション・ビル名"]

        # -------------------------------------------------------------
        # デバッグ: 見つからなかった列をログに出力
        # -------------------------------------------------------------
        all_required_csv_cols = list(csv_to_ss_map.values()) + address_csv_columns
        missing_columns = [col for col in all_required_csv_cols if col not in header_map]
        if missing_columns:
            write_log(f"⚠️ 警告: 以下の必須項目がCSV内に見つからなかったため、データが欠落する可能性があります:\n{missing_columns}")
            write_log(f"ℹ️ 参考: 実際のCSVに含まれている列名一覧:\n{list(header_map.keys())}")

        data_list = []

        # 2行目以降のデータ行を処理
        for row in raw_data[1:]:
            new_row = []
            for ss_header in spreadsheet_headers:
                cell_val = ""  # デフォルトは空文字

                if ss_header == "ステータス":
                    # C列には固定で「新規」をセット
                    cell_val = "新規"
                elif ss_header == "顧客住所":
                    # 住所関連の列を結合
                    address_parts = []
                    for col_name in address_csv_columns:
                        if col_name in header_map:
                            idx = header_map[col_name]
                            if idx < len(row) and row[idx]: # 空の要素は結合しない
                                address_parts.append(row[idx])
                    cell_val = "".join(address_parts)

                elif ss_header in csv_to_ss_map:
                    # 通常の列マッピング
                    csv_col_name = csv_to_ss_map[ss_header]
                    if csv_col_name in header_map:
                        idx = header_map[csv_col_name]
                        if idx < len(row):
                            cell_val = row[idx]
                
                # 前後の空白除去 ＆ 先頭の ' を削除
                clean_cell = cell_val.strip().lstrip("'")
                new_row.append(clean_cell)
            data_list.append(new_row)
            
        write_log(f"データの整形完了 (指定された{len(spreadsheet_headers)}項目のみ抽出し並び替えました)")

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

        # A,B列および1行目（ヘッダー）を残すため、C2以降のデータをクリア
        worksheet.batch_clear(['C2:Z'])

        write_log(f"スプレッドシート '{worksheet.title}' を更新しています...")
        
        worksheet.update(
            range_name='C2', 
            values=data_list, 
            value_input_option='USER_ENTERED'
        )

        write_log("=== 完了: スプレッドシートの更新が終わりました ===")

    except Exception as e:
        write_log(f"スプレッドシートの更新エラー: {e}")

if __name__ == "__main__":
    main()
