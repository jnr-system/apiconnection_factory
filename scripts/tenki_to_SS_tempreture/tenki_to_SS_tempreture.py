"""
【実行内容】
気象庁の過去の気象データ検索ページから、前日の各都道府県（主要都市）の最低気温を取得し、
Googleスプレッドシートの指定された月別シート（例: "1月_new"）の日付列に書き込みます。
"""
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import rowcol_to_a1, a1_to_rowcol
import time
from pathlib import Path
from datetime import datetime, timedelta
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
SPREADSHEET_KEY = "1jmBAudOWcED7D9jIxAVvpXBfvwgJIV4B1TEJEfkVs-w"

# ★「北海道の1日」が始まるセル位置（ここを基準に列をずらします）
BASE_CELL = "B62"

LOG_FILE_PATH = Path(__file__).parent / "execution_log.txt"

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
# 都道府県リスト
# ==============================================================================
ALL_CITIES = [
    {"name": "北海道", "prec_no": "14", "block_no": "47412"},
    {"name": "青森県", "prec_no": "31", "block_no": "47575"},
    {"name": "岩手県", "prec_no": "33", "block_no": "47584"},
    {"name": "宮城県", "prec_no": "34", "block_no": "47590"},
    {"name": "秋田県", "prec_no": "32", "block_no": "47582"},
    {"name": "山形県", "prec_no": "35", "block_no": "47588"},
    {"name": "福島県", "prec_no": "36", "block_no": "47595"},
    {"name": "茨城県", "prec_no": "40", "block_no": "47629"},
    {"name": "栃木県", "prec_no": "41", "block_no": "47615"},
    {"name": "群馬県", "prec_no": "42", "block_no": "47624"},
    {"name": "埼玉県", "prec_no": "43", "block_no": "47626"},
    {"name": "千葉県", "prec_no": "45", "block_no": "47682"},
    {"name": "東京都", "prec_no": "44", "block_no": "47662"},
    {"name": "神奈川県", "prec_no": "46", "block_no": "47670"},
    {"name": "新潟県", "prec_no": "54", "block_no": "47604"},
    {"name": "富山県", "prec_no": "55", "block_no": "47607"},
    {"name": "石川県", "prec_no": "56", "block_no": "47605"},
    {"name": "福井県", "prec_no": "57", "block_no": "47616"},
    {"name": "山梨県", "prec_no": "49", "block_no": "47638"},
    {"name": "長野県", "prec_no": "48", "block_no": "47610"},
    {"name": "岐阜県", "prec_no": "52", "block_no": "47632"},
    {"name": "静岡県", "prec_no": "50", "block_no": "47656"},
    {"name": "愛知県", "prec_no": "51", "block_no": "47636"},
    {"name": "三重県", "prec_no": "53", "block_no": "47651"},
    {"name": "滋賀県", "prec_no": "60", "block_no": "47761"},
    {"name": "京都府", "prec_no": "61", "block_no": "47759"},
    {"name": "大阪府", "prec_no": "62", "block_no": "47772"},
    {"name": "兵庫県", "prec_no": "63", "block_no": "47770"},
    {"name": "奈良県", "prec_no": "64", "block_no": "47780"},
    {"name": "和歌山県", "prec_no": "65", "block_no": "47777"},
    {"name": "鳥取県", "prec_no": "69", "block_no": "47746"},
    {"name": "島根県", "prec_no": "68", "block_no": "47741"},
    {"name": "岡山県", "prec_no": "66", "block_no": "47768"},
    {"name": "広島県", "prec_no": "67", "block_no": "47765"},
    {"name": "山口県", "prec_no": "81", "block_no": "47784"},
    {"name": "徳島県", "prec_no": "71", "block_no": "47895"},
    {"name": "香川県", "prec_no": "72", "block_no": "47891"},
    {"name": "愛媛県", "prec_no": "73", "block_no": "47887"},
    {"name": "高知県", "prec_no": "74", "block_no": "47893"},
    {"name": "福岡県", "prec_no": "82", "block_no": "47807"},
    {"name": "佐賀県", "prec_no": "85", "block_no": "47813"},
    {"name": "長崎県", "prec_no": "84", "block_no": "47817"},
    {"name": "熊本県", "prec_no": "86", "block_no": "47819"},
    {"name": "大分県", "prec_no": "83", "block_no": "47815"},
    {"name": "宮崎県", "prec_no": "87", "block_no": "47830"},
    {"name": "鹿児島県", "prec_no": "88", "block_no": "47827"},
    {"name": "沖縄県", "prec_no": "91", "block_no": "47936"}
]

# ==============================================================================
# データ取得関数
# ==============================================================================
def scrape_jma_target_day(city_config, year, month, target_day):
    """指定された日のデータだけを抽出して返す"""
    prec = city_config["prec_no"]
    block = city_config["block_no"]
    # サイト構造上、月間一覧ページが最も軽量で取得しやすいためそこから取得
    url = f"https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no={prec}&block_no={block}&year={year}&month={month}&day=&view="

    try:
        dfs = pd.read_html(url, header=0)
        df = dfs[0]
        
        # 必要な列（日付と最低気温）のみ抽出
        # ※サイトの列構成: 0=日付, 8=最低気温
        target_df = df.iloc[:, [0, 8]].copy()
        target_df.columns = ["day", "min_temp"]
        
        # 文字列型に統一
        target_df["day"] = target_df["day"].astype(str)
        target_df["min_temp"] = target_df["min_temp"].astype(str)

        # ターゲット日の行を検索
        # "1" とか "15" という文字列でマッチング
        row = target_df[target_df["day"] == str(target_day)]
        
        if row.empty:
            return "-"
        
        val = row.iloc[0]["min_temp"]
        
        # --- クリーニング ---
        val = val.replace("nan", "-")
        val = val.replace("'", "").replace("]", "").replace(")", "")
        if not val.strip():
            return "-"
            
        return val

    except Exception:
        return "-"

# ==============================================================================
# メイン処理
# ==============================================================================
def main():
    # 1. 前日を計算
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    target_year = yesterday.year
    target_month = yesterday.month
    target_day = yesterday.day
    
    sheet_name = f"{target_month}月_new"

    write_log(f"=== 天気データ取得処理を開始します ===")
    write_log(f"{target_year}年{target_month}月{target_day}日分のデータを取得します -> シート名: {sheet_name}")


    # 2. 書き込み位置の計算
    # BASE_CELL (B62) は "1日" の列。
    # ターゲット日が "N日" なら、列を (N-1) だけ右にずらす。
    base_row, base_col = a1_to_rowcol(BASE_CELL)
    
    target_col = base_col + (target_day - 1)
    target_cell_a1 = rowcol_to_a1(base_row, target_col)
    
    write_log(f"書き込み開始セル: {target_cell_a1} (日付: {target_day}日)")

    # 3. データ収集（前日分のみ）
    # 縦一列分のリストを作成する [[Hokkaido], [Aomori], ...]
    col_data = []

    for city in ALL_CITIES:
        val = scrape_jma_target_day(city, target_year, target_month, target_day)
        col_data.append([val]) # gspreadは [[val1], [val2]] の形式で行ごとの値を表現する
        time.sleep(0.1) # サーバー負荷軽減

    # 4. 書き込み
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        workbook = client.open_by_key(SPREADSHEET_KEY)

        try:
            sheet = workbook.worksheet(sheet_name)
        except:
            write_log(f"シート '{sheet_name}' がないため新規作成します。")
            sheet = workbook.add_worksheet(title=sheet_name, rows="100", cols="40")

        # ピンポイント書き込み（1列×47行分）
        sheet.update(range_name=target_cell_a1, values=col_data)
        
        # 右寄せ（書き込んだ列だけ）
        # 書き込み範囲の終了行を計算
        end_row = base_row + len(col_data) - 1
        range_string = f"{target_cell_a1}:{rowcol_to_a1(end_row, target_col)}"
        
        sheet.format(range_string, {"horizontalAlignment": "RIGHT"})
        
        write_log(f"完了: スプレッドシートへ書き込みました。")

    except Exception as e:
        write_log(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()