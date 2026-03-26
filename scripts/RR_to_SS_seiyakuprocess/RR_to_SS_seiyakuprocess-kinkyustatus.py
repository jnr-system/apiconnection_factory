import pandas as pd
import requests
import io
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from pathlib import Path
import os

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 本番環境では python-dotenv がなくても動作する

# ==============================================================================
# ■ 設定エリア
# ==============================================================================

# 1. 集計期間の設定
# 指定がある場合はその期間を集計します（YYYY/MM/DD形式）
# 指定がない場合（None または ""）は、実行日の「前日」を自動的に対象とします
TARGET_DATE_START = ""
TARGET_DATE_END   = ""

# 2. Googleスプレッドシート設定
SPREADSHEET_KEY = "19l5TYkXN1SdwrWNVkgKHLymx4_chqSunOrY-2rAqW14" # ★スプレッドシートID

# 3. 楽楽販売 API設定
RR_DOMAIN  = "hntobias.rakurakuhanbai.jp"
RR_ACCOUNT = "mspy4wa"
RR_TOKEN   = os.environ["RAKURAKU_TOKEN"]
RR_API_URL = f"https://{RR_DOMAIN}/{RR_ACCOUNT}/api/csvexport/version/v1"

# 4. 楽楽販売 DB設定 (問い合わせ管理)
DB_INQUIRY = {
    "dbSchemaId": "101181", # ★問い合わせ管理のSchemaID
    "listId":     "101452", # ★【変更してください】利用する一覧のID
    "searchIds": [
        "107226",           # ★【変更してください】検索条件ID
    ],
    "cols": {
        "id":      "記録ID",
        "date":    "新規問い合わせ日",
        "status":  "対応状況ステータス",
        "urgency": "緊急度", # ★追加された項目
        "product_type": "新規商品タイプ"
    }
}

# 4-2. 1回のAPIリクエストで取得する最大件数
LIMIT_PER_REQUEST = 10000

# 5. スプレッドシートの書き込み位置設定
COL_START_ALL     = 4   # D列
COL_START_KYUTOKI = 38  # AL列
COL_START_ECO     = 72  # BT列

# ★小項目ステータスのグループ定義
# 各小項目名と、それに該当するステータス名のリスト
STATUS_GROUPS = [
    ("一次受付", ["1 ■一次受付（ヒアリング）"]),
    ("概算見積", ["1-10 ■概算見積（写真フォーム案内なし）", "1-11 ■概算見積（写真待ち）", "1-20 ■確認中（施工可否、現調含む）", "1-40 ■現地調査（成約前）"]),
    ("最終見積", ["1-30 ■最終見積り済"]),
    ("成約",     ["当日成約", "当日外成約"]),
    ("失注",     ["1-61-A ■失注：他社（価格）", "1-61-B ■失注：他社（スピード）", "1-61-C ■失注：交換見送り", "1-61-D ■失注：その他・理由不明", "1-61-E ■失注：終了"]),
    ("キャンセル", ["1-60 ■キャンセル（成約からキャンセル）"]),
    ("対応不可", ["1-62 ■エリア外、施工・対応不可"]),
]

# ★緊急度ごとの設定（検索キーワードと書き込み開始行番号）
# 小項目は STATUS_GROUPS の順に row_cum_start / row_day_start から連続して書き込む
URGENCY_CONFIG = [
    {"name": "急ぎ",       "keyword": "急ぎ",       "row_cum_start": 7,  "row_day_start": 34},
    {"name": "故障",       "keyword": "故障",       "row_cum_start": 15, "row_day_start": 42},
    {"name": "不具合・検討", "keyword": "不具合・検討", "row_cum_start": 23, "row_day_start": 50},
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
    """楽楽販売APIからデータを取得する"""
    headers = {"X-HD-apitoken": RR_TOKEN, "Content-Type": "application/json"}
    all_dfs = []

    for search_id in settings["searchIds"]:
        payload = {
            "dbSchemaId": settings["dbSchemaId"],
            "listId":     settings["listId"],
            "searchId":   search_id,
            "limit":      LIMIT_PER_REQUEST,
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
            # 列名の前後の空白を削除
            df.columns = df.columns.str.replace('　', ' ').str.strip()

            write_log(f"  → {len(df)}件取得")
            if len(all_dfs) == 0:
                write_log(f"  [DEBUG] 取得したCSVの列名一覧: {list(df.columns)}")

            all_dfs.append(df)

        except Exception as e:
            write_log(f"接続エラー: {e}")

    if not all_dfs:
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=[settings["cols"]["id"]])
    write_log(f"合計（重複除去後）: {len(combined_df)}件")
    return combined_df

def raw_to_df(df_raw):
    """取得した生データをDataFrameに変換する"""
    cols = DB_INQUIRY["cols"]
    records = []

    for _, row in df_raw.iterrows():
        record_id = str(row.get(cols["id"], "")).strip()
        if not record_id or record_id == "nan":
            continue

        status       = str(row.get(cols["status"], "")).strip()
        urgency      = str(row.get(cols["urgency"], "")).strip()
        product_type = str(row.get(cols["product_type"], "")).strip()
        date_str     = str(row.get(cols["date"], ""))[:10].replace("/", "-")

        date_obj = None
        try:
            if date_str and len(date_str) >= 10:
                date_obj = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except ValueError:
            pass

        records.append({
            "最終ステータス": status,
            "緊急度判定":     urgency if urgency != "nan" else "",
            "商品タイプ":     product_type if product_type != "nan" else "",
            "日付_単体":      date_obj,
        })

    return pd.DataFrame(records) if records else pd.DataFrame()

def count_status_group(df, statuses):
    """指定ステータスリストに一致する件数を返す"""
    if df.empty:
        return 0
    return int(df["最終ステータス"].isin(statuses).sum())

def update_spreadsheet_cells(df, target_dates):
    """スプレッドシートを更新する"""
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

    cells_to_update_by_sheet = {}
    sheet_cache = {}

    # 商品タイプごとの設定 (名称, 開始列, 検索キーワード)
    # キーワードが空文字の場合は商品タイプでフィルタしない（全体集計）
    product_categories = [
        ("全体",       COL_START_ALL,     ""),
        ("給湯器",     COL_START_KYUTOKI, "給湯器"),
        ("エコキュート", COL_START_ECO,     "エコ")
    ]

    for d in target_dates:
        sheet_name = f"{d.month}月"

        if sheet_name not in sheet_cache:
            try:
                sheet_cache[sheet_name] = workbook.worksheet(sheet_name)
                cells_to_update_by_sheet[sheet_name] = []
            except Exception as e:
                write_log(f"警告: シート '{sheet_name}' が見つかりません。")
                sheet_cache[sheet_name] = None

        if not sheet_cache[sheet_name]:
            continue

        # ── 日付フィルタリング ──────────────────────────────
        # 累計の集計期間（15日前から当日まで）
        start_window = d - timedelta(days=14)

        # 単日データ（その日のデータ）
        subset_day = df[df["日付_単体"] == d] if not df.empty else pd.DataFrame()

        # 累計データ（15日前から当日まで）
        subset_cum = df[(df["日付_単体"] >= start_window) & (df["日付_単体"] <= d)] if not df.empty else pd.DataFrame()

        for prod_name, start_col, prod_keyword in product_categories:
            target_col = start_col + (d.day - 1)

            # 商品タイプでフィルタ（キーワード空の場合は全件対象）
            if prod_keyword:
                subset_day_prod = subset_day[subset_day["商品タイプ"].str.contains(prod_keyword, na=False)] if not subset_day.empty else pd.DataFrame()
                subset_cum_prod = subset_cum[subset_cum["商品タイプ"].str.contains(prod_keyword, na=False)] if not subset_cum.empty else pd.DataFrame()
            else:
                subset_day_prod = subset_day
                subset_cum_prod = subset_cum

            for config in URGENCY_CONFIG:
                keyword = config["keyword"]

                # 緊急度でフィルタ
                subset_day_urg = subset_day_prod[subset_day_prod["緊急度判定"].str.contains(keyword, na=False)] if not subset_day_prod.empty else pd.DataFrame()
                subset_cum_urg = subset_cum_prod[subset_cum_prod["緊急度判定"].str.contains(keyword, na=False)] if not subset_cum_prod.empty else pd.DataFrame()

                # 小項目ごとに書き込み
                for i, (_, statuses) in enumerate(STATUS_GROUPS):
                    row_day = config["row_day_start"] + i
                    row_cum = config["row_cum_start"] + i

                    val_day = count_status_group(subset_day_urg, statuses)
                    val_cum = count_status_group(subset_cum_urg, statuses)

                    cells_to_update_by_sheet[sheet_name].append(
                        gspread.Cell(row=row_day, col=target_col, value=val_day if val_day > 0 else "")
                    )
                    cells_to_update_by_sheet[sheet_name].append(
                        gspread.Cell(row=row_cum, col=target_col, value=val_cum if val_cum > 0 else "")
                    )

    for sheet_name_key, cells in cells_to_update_by_sheet.items():
        if cells:
            write_log(f"シート '{sheet_name_key}' にデータを一括上書きします...")
            sheet_cache[sheet_name_key].update_cells(cells)

    write_log("スプレッドシートの更新がすべて完了しました！")

# ==============================================================================
# ■ メイン処理
# ==============================================================================

def main():
    # ── 期間設定 ────────────────────────────────────────
    if TARGET_DATE_START and TARGET_DATE_END:
        start_date = datetime.strptime(TARGET_DATE_START, "%Y/%m/%d").date()
        end_date   = datetime.strptime(TARGET_DATE_END,   "%Y/%m/%d").date()
    else:
        # 自動モード：当日のみを集計対象とする
        start_date = datetime.now().date()
        end_date   = start_date

    days = (end_date - start_date).days + 1
    target_dates = [start_date + timedelta(days=i) for i in range(days)]

    # 累計用データの取得開始日（集計日の15日前）
    cum_start = start_date - timedelta(days=14)

    write_log(f"集計対象期間: {start_date} 〜 {end_date} ({days}日間)")
    write_log(f"データ取得範囲: {cum_start} 以降 (累計計算用)")

    # ── 1. 問い合わせ管理DBからの取得 ────────────────────────
    write_log(">>> 問い合わせ管理DBの取得を開始します")
    df_raw = fetch_rakuraku_csv(DB_INQUIRY)

    if df_raw.empty:
        write_log("データが取得できませんでした。処理を終了します。")
        return

    df = raw_to_df(df_raw)
    write_log(f"変換後: {len(df)}件")

    # ── 2. スプレッドシート更新 ──────────────────────────────
    update_spreadsheet_cells(df, target_dates)

    write_log("全処理完了。")

if __name__ == "__main__":
    main()
