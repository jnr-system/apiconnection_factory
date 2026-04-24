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

# ★各データの「北海道の1日」が始まるセル位置（ここを基準に列をずらします）
BASE_CELL_MIN_TEMP   = "B3"   # 最低気温
BASE_CELL_PRECIP     = "B51"  # 降水量（合計）
BASE_CELL_HUMIDITY   = "B99"  # 湿度（平均）

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
    """指定された日の最低気温・降水量合計・湿度平均を返す"""
    prec = city_config["prec_no"]
    block = city_config["block_no"]
    url = f"https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no={prec}&block_no={block}&year={year}&month={month}&day=&view="

    try:
        dfs = pd.read_html(url, header=0)
        df = dfs[0]

        # ※サイトの列構成: 0=日付, 3=降水量合計, 8=最低気温, 9=湿度平均
        target_df = df.iloc[:, [0, 3, 8, 9]].copy()
        target_df.columns = ["day", "precip", "min_temp", "humidity"]

        target_df["day"] = target_df["day"].astype(str)

        row = target_df[target_df["day"] == str(target_day)]

        if row.empty:
            return "-", "-", "-"

        def clean(val):
            val = str(val).replace("nan", "-").replace("'", "").replace("]", "").replace(")", "").strip()
            return "-" if not val else val

        def clean_precip(val):
            val = str(val).replace("nan", "0.0").replace("'", "").replace("]", "").replace(")", "").strip()
            if not val or val in ("--", "-"):
                return "0.0"
            return val

        return clean(row.iloc[0]["min_temp"]), clean_precip(row.iloc[0]["precip"]), clean(row.iloc[0]["humidity"])

    except Exception:
        return "-", "-", "-"

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


    # 2. 書き込み位置の計算（各データ種別ごと）
    col_offset = target_day - 1

    def calc_cell(base_cell):
        r, c = a1_to_rowcol(base_cell)
        return r, c + col_offset

    r_temp,     c_temp     = calc_cell(BASE_CELL_MIN_TEMP)
    r_precip,   c_precip   = calc_cell(BASE_CELL_PRECIP)
    r_humidity, c_humidity = calc_cell(BASE_CELL_HUMIDITY)

    cell_temp     = rowcol_to_a1(r_temp,     c_temp)
    cell_precip   = rowcol_to_a1(r_precip,   c_precip)
    cell_humidity = rowcol_to_a1(r_humidity, c_humidity)

    write_log(f"書き込みセル: 最低気温={cell_temp}, 降水量={cell_precip}, 湿度={cell_humidity} (日付: {target_day}日)")

    # 3. データ収集（前日分のみ）
    col_min_temp  = []
    col_precip    = []
    col_humidity  = []

    for city in ALL_CITIES:
        min_temp, precip, humidity = scrape_jma_target_day(city, target_year, target_month, target_day)
        col_min_temp.append([min_temp])
        col_precip.append([precip])
        col_humidity.append([humidity])
        time.sleep(0.1)

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
            sheet = workbook.add_worksheet(title=sheet_name, rows="150", cols="40")

        def write_col(start_cell, start_row, start_col, data):
            sheet.update(range_name=start_cell, values=data)
            end_row = start_row + len(data) - 1
            sheet.format(f"{start_cell}:{rowcol_to_a1(end_row, start_col)}", {"horizontalAlignment": "RIGHT"})

        write_col(cell_temp,     r_temp,     c_temp,     col_min_temp)
        write_col(cell_precip,   r_precip,   c_precip,   col_precip)
        write_col(cell_humidity, r_humidity, c_humidity, col_humidity)

        write_log(f"完了: スプレッドシートへ書き込みました。")

    except Exception as e:
        write_log(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()