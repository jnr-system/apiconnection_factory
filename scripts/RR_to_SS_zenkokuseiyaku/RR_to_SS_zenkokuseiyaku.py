"""
【実行内容】
楽楽販売APIから成約データを取得し、商品タイプ（給湯器、エコキュート、コンロ等）や都道府県を判定して集計します。
集計結果はGoogleスプレッドシートの「全体」シート（月別合計）および月別シート（日別・都道府県別件数）に反映させます。
"""
import pandas as pd
import requests
import io
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from pathlib import Path
import os
import json

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 本番環境では python-dotenv がなくても動作する

# ==============================================================================
# ■ 設定エリア
# ==============================================================================

# 1. 実行対象の「基準日」指定 (YYYY/MM/DD 形式)
#    ★ 空欄 "" にすると自動的に「前日」が基準になり、そこから過去10日分を更新します。
MANUAL_TARGET_DATE = ""  
DAYS_TO_SYNC = 1 # 遡る日数

# 2. Googleスプレッドシート設定
SPREADSHEET_KEY = "1azX9nlrTSs9sZP-abRHbW5BuS8OpIT_7Vb3OCi7pZFA"
#https://docs.google.com/spreadsheets/d/1azX9nlrTSs9sZP-abRHbW5BuS8OpIT_7Vb3OCi7pZFA/edit?gid=1670992266#gid=1670992266
SHEET_NAME_OVERALL = "全体" # ★全体集計用のシート名
# ※シート名は日付から自動で「1月」「2月」と判別して書き込みます。

# 3. 楽楽販売 API設定
RR_DOMAIN  = "hntobias.rakurakuhanbai.jp"
RR_ACCOUNT = "mspy4wa"
RR_TOKEN   = os.environ["RAKURAKU_TOKEN"]
RR_API_URL = f"https://{RR_DOMAIN}/{RR_ACCOUNT}/api/csvexport/version/v1"

# 4. 楽楽販売 DB設定 (成約管理)
DB_SEIYAKU = {
    "dbSchemaId": "101185", # ★成約管理のSchemaID
    "listId":     "101454", # ★利用する一覧のID
    "searchIds":  ["107200", "107191"], # ★先月用と今月用の検索条件IDを2つ設定！
    "cols": {
        "id":       "手配番号",      
        "date":     "成約日",      
        "pref":     "（日程調整）施工先都道府県",  
        "address":  "（日程調整）住所", 
        "type":     "商品タイプ",
        "name":     "商品名",
        "price":    "（日程調整）請求金額（税込）"
    }
}

# 5. スプレッドシートの書き込み位置設定
# --- 都道府県別データ ---
COL_START_GAS = 3   # C列 (給湯器の1日)
COL_START_ECO = 35  # AI列 (エコキュートの1日)
START_ROW     = 6   # 6行目 (北海道の行)

# --- 全体シート設定 (月別合計) ---
# 行の設定 (給湯器=4行目, エコキュート=5行目...)
OVERALL_ROWS = {
    "給湯器": 4,
    "エコキュート": 5,
    "コンロ": 6,
    "その他": 7
}

PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県", "不明"
]

# ==============================================================================
# ■ 関数群
# ==============================================================================

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

def fetch_rakuraku_csv(settings):
    headers = {"X-HD-apitoken": RR_TOKEN, "Content-Type": "application/json"}
    all_dfs = []
    
    for search_id in settings["searchIds"]:
        payload = {
            "dbSchemaId": settings["dbSchemaId"],
            "listId":     settings["listId"],
            "searchId":   search_id,
            "limit":      10000 
        }
        write_log(f"データ取得中... (SearchID: {search_id})")
        try:
            res = requests.post(RR_API_URL, headers=headers, json=payload, timeout=60)
            if res.status_code != 200:
                write_log(f"APIエラー: {res.status_code}")
                continue
            try: content = res.content.decode("cp932")
            except: content = res.content.decode("utf-8", errors="ignore")
            
            df = pd.read_csv(io.StringIO(content), dtype=str)
            all_dfs.append(df)
        except Exception as e:
            write_log(f"接続エラー: {e}")
            
    if not all_dfs:
        return pd.DataFrame()
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df = combined_df.drop_duplicates()
    return combined_df

def determine_prefecture(row):
    cols = DB_SEIYAKU["cols"]
    pref_val = str(row.get(cols["pref"], "")).strip()
    if pref_val and pref_val != "nan" and pref_val in PREFECTURES:
        return pref_val
    
    address = str(row.get(cols["address"], "")).strip()
    if address and address != "nan":
        for p in PREFECTURES:
            if p in address:
                return p
    return "不明"

def determine_row_product_type(row):
    """【行単位】の商品タイプ推測 (コンロ・その他を含む4分類)"""
    cols = DB_SEIYAKU["cols"]
    p_type = str(row.get(cols["type"], "")).strip()
    p_name = str(row.get(cols["name"], "")).strip().upper()

    # 1. 【最優先】コンロのブロック
    stove_keywords = ["コンロ", "N3W", "RS31", "PD-", "RHS", "N3S", "ビルトイン"]
    if any(kw in p_type for kw in stove_keywords) or any(kw in p_name for kw in stove_keywords):
        return "コンロ"

    # 2. 給湯器・エコキュートの文字判定
    if "給湯器" in p_type: return "給湯器"
    if "エコキュート" in p_type or "電気温水器" in p_type: return "エコキュート"

    # 3. 給湯器・エコキュートの型番判定
    eco_keywords = ["エコキュート", "SRT-", "EQ", "HE-", "BHP-", "HWH-", "CHP-", "電気温水器"]
    if any(kw in p_name for kw in eco_keywords): return "エコキュート"
        
    gas_keywords = ["給湯器", "GT-", "RUF-", "GQ-", "GTH-", "RVD-", "RUX-", "FH-", "PH-"]
    if any(kw in p_name for kw in gas_keywords): return "給湯器"

    # 4. 金額判定
    try:
        price_str = str(row.get(cols["price"], "0")).replace(",", "")
        price = float(price_str) if price_str and price_str != "nan" else 0
    except:
        price = 0

    if price >= 300000: return "エコキュート"
    elif price >= 100000: return "給湯器"

    return "その他"

def process_sales_data(df):
    cols = DB_SEIYAKU["cols"]
    
    df["成約日_dt"] = pd.to_datetime(df[cols["date"]], errors="coerce")
    df = df.dropna(subset=["成約日_dt"])
    
    if df.empty:
        write_log(f"成約データはありませんでした。")
        return pd.DataFrame()

    df["行の推測"] = df.apply(determine_row_product_type, axis=1)

    # 手配番号ごとに「契約の主役」を決める（エコキュート優先 > 給湯器 > コンロ > その他）
    def decide_contract_type(group):
        types = group["行の推測"].tolist()
        if "エコキュート" in types: return "エコキュート"
        if "給湯器" in types: return "給湯器"
        if "コンロ" in types: return "コンロ"
        return "その他"

    contract_types = df.groupby(cols["id"]).apply(decide_contract_type).reset_index(name="最終商品タイプ")
    contract_prefs = df.groupby(cols["id"]).apply(lambda g: determine_prefecture(g.iloc[0])).reset_index(name="最終都道府県")
    contract_dates = df.groupby(cols["id"])["成約日_dt"].first().reset_index(name="成約日")

    df_unique = pd.merge(contract_types, contract_prefs, on=cols["id"])
    df_unique = pd.merge(df_unique, contract_dates, on=cols["id"])
    df_unique["成約日_日付"] = df_unique["成約日"].dt.date
    
    return df_unique

def update_spreadsheet_cells(df_target, target_dates):
    write_log(f"スプレッドシートを更新中...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    try:
        creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        workbook = client.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        write_log(f"スプレッドシート接続エラー: {e}")
        return

    # ---------------------------------------------------------
    # 1. 全体シートの更新 (月別合計)
    # ---------------------------------------------------------
    try:
        sheet_overall = workbook.worksheet(SHEET_NAME_OVERALL)
        overall_cells = []
        
        # 月ごとに集計
        df_target["month"] = df_target["成約日"].dt.month
        monthly_counts = df_target.groupby(["month", "最終商品タイプ"]).size()
        
        for month in df_target["month"].unique():
            col_idx = int(month) + 2  # 1月=C(3), 2月=D(4)...
            
            for type_name, row_idx in OVERALL_ROWS.items():
                count = int(monthly_counts.get((month, type_name), 0))
                # ★入力されている値に加算ではなく、DBの正解値で上書き更新します（二重計上防止のため）
                overall_cells.append(gspread.Cell(row=row_idx, col=col_idx, value=count))
        
        if overall_cells:
            write_log(f"シート '{SHEET_NAME_OVERALL}' の月別合計を更新します...")
            sheet_overall.update_cells(overall_cells)
    except Exception as e:
        write_log(f"全体シート更新エラー (シート名 '{SHEET_NAME_OVERALL}' が存在するか確認してください): {e}")

    # ---------------------------------------------------------
    # 2. 日別・都道府県別シートの更新
    # ---------------------------------------------------------
    cells_to_update_by_sheet = {}
    sheet_cache = {}

    for d in target_dates:
        # ★ ここを実際のシート名フォーマットに合わせています（エラーログを見ると "2月_" になっていたため）
        # もしスプレッドシートのタブ名が「2月」なら f"{d.month}月" にしてください。
        sheet_name = f"{d.month}月"
        
        if sheet_name not in sheet_cache:
            try:
                sheet_cache[sheet_name] = workbook.worksheet(sheet_name)
                cells_to_update_by_sheet[sheet_name] = []
            except Exception as e:
                write_log(f"警告: シート '{sheet_name}' が見つからないためスキップします。")
                sheet_cache[sheet_name] = None
                
        if not sheet_cache[sheet_name]:
            continue
            
        target_day = d.day
        subset = df_target[df_target["成約日_日付"] == d] if not df_target.empty else pd.DataFrame()
        
        if not subset.empty:
            counts = subset.groupby(["最終都道府県", "最終商品タイプ"]).size()
        else:
            counts = {}

        # 都道府県別の処理（給湯器、エコキュート）
        for i, pref in enumerate(PREFECTURES):
            row_idx = START_ROW + i
            
            # 給湯器 (C列=3 〜)
            col_gas = COL_START_GAS + (target_day - 1)
            # ★ ここも int() で変換！
            gas_count = int(counts.get((pref, "給湯器"), 0))
            cells_to_update_by_sheet[sheet_name].append(gspread.Cell(row=row_idx, col=col_gas, value=gas_count if gas_count > 0 else "-"))
            
            # エコキュート (AI列=35 〜)
            col_eco = COL_START_ECO + (target_day - 1)
            # ★ ここも int() で変換！
            eco_count = int(counts.get((pref, "エコキュート"), 0))
            cells_to_update_by_sheet[sheet_name].append(gspread.Cell(row=row_idx, col=col_eco, value=eco_count if eco_count > 0 else "-"))

    # 各シートごとにまとめて一括更新
    for sheet_name, cells in cells_to_update_by_sheet.items():
        if cells:
            write_log(f"シート '{sheet_name}' にデータを一括上書きします...")
            sheet_cache[sheet_name].update_cells(cells)

    write_log("スプレッドシートの更新がすべて完了しました！")

# ==============================================================================
# ■ メイン処理
# ==============================================================================

def main():
    today = datetime.now()
    if MANUAL_TARGET_DATE:
        base_date = datetime.strptime(MANUAL_TARGET_DATE, "%Y/%m/%d").date()
    else:
        base_date = (today - timedelta(days=1)).date()

    target_dates = [(base_date - timedelta(days=i)) for i in range(DAYS_TO_SYNC)]
    target_dates.reverse()

    write_log(f"集計期間: {target_dates[0]} 〜 {target_dates[-1]} ({DAYS_TO_SYNC}日間)")

    df_raw = fetch_rakuraku_csv(DB_SEIYAKU)
    if df_raw.empty:
        write_log("処理を終了します。")
        return

    df_target = process_sales_data(df_raw)
    update_spreadsheet_cells(df_target, target_dates)
    
    write_log("全処理完了。")

if __name__ == "__main__":
    main()