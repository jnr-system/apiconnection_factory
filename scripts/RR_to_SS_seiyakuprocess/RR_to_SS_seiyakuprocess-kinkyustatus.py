import calendar
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

# 1-2. スナップショット保存先（スクリプトと同じフォルダ内の snapshots/ フォルダ）
SNAPSHOT_DIR = Path(__file__).parent / "snapshots"

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
COL_START_ALL     = 6   # ★全体の開始列 (F列=6)
COL_START_KYUTOKI = 42  # ★給湯器の開始列 (AP列=42)
COL_START_ECO     = 78  # ★エコキュートの開始列 (BZ列=78)

# ★緊急度ごとの設定（検索キーワードと書き込み行番号）
URGENCY_CONFIG = [
    {
        "name": "急ぎ", "keyword": "急ぎ",
        "rows_cum": {"UU数": 6,  "成約数": 7,  "終了数": 8},  # 累積 (行6-8)
        "rows_day": {"UU数": 32, "成約数": 33, "終了数": 34}  # 単日 (行32-34)
    },
    {
        "name": "故障", "keyword": "故障",
        "rows_cum": {"UU数": 9,  "成約数": 10, "終了数": 11}, # 累積 (行9-11)
        "rows_day": {"UU数": 35, "成約数": 36, "終了数": 37}  # 単日 (行35-37)
    },
    {
        "name": "不具合・検討", "keyword": "不具合・検討",
        "rows_cum": {"UU数": 12, "成約数": 13, "終了数": 14}, # 累積 (行12-14)
        "rows_day": {"UU数": 38, "成約数": 39, "終了数": 40}  # 単日 (行38-40)
    }
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

# ==============================================================================
# ■ スナップショット関連
# ==============================================================================

def save_snapshot(df_raw, snapshot_date):
    """
    問い合わせ管理DBの全件スナップショットをJSONに保存する。
    ファイル名: snapshots/YYYY-MM-DD.json
    内容: {記録ID: {status, urgency, product_type, date}}
    """
    SNAPSHOT_DIR.mkdir(exist_ok=True)

    cols = DB_INQUIRY["cols"]
    data = {}

    for _, row in df_raw.iterrows():
        record_id = str(row.get(cols["id"], "")).strip()
        if not record_id or record_id == "nan":
            continue

        status       = str(row.get(cols["status"], "")).strip()
        urgency      = str(row.get(cols["urgency"], "")).strip()
        product_type = str(row.get(cols["product_type"], "")).strip()
        # 日付形式を YYYY-MM-DD に統一
        date_str     = str(row.get(cols["date"], ""))[:10].replace("/", "-")

        data[record_id] = {
            "status":       status,
            "urgency":      urgency if urgency != "nan" else "",
            "product_type": product_type if product_type != "nan" else "",
            "date":         date_str,
        }

    file_path = SNAPSHOT_DIR / f"{snapshot_date}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    write_log(f"スナップショット保存: {file_path.name} ({len(data)}件)")
    return data

def snapshot_to_df(snapshot_data, cum_start=None):
    """
    スナップショットデータをDataFrameに変換する。
    cum_start を指定すると「新規問い合わせ日 >= cum_start」の案件のみを残す。
    """
    if not snapshot_data:
        return pd.DataFrame()

    records = []
    for _, info in snapshot_data.items():
        date_str = info.get("date", "")
        date_obj = None
        try:
            if date_str and len(date_str) >= 10:
                date_obj = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except ValueError:
            pass

        # 累計フィルタ
        if cum_start is not None:
            if date_obj is None or date_obj < cum_start:
                continue

        records.append({
            "最終ステータス": info.get("status", ""),
            "緊急度判定":     info.get("urgency", ""),
            "商品タイプ":     info.get("product_type", ""),
            "日付_単体":      date_obj,
        })

    return pd.DataFrame(records) if records else pd.DataFrame()

def load_snapshot_for_date(target_date):
    """指定日のスナップショットJSONがあれば読み込んでDataFrameにする"""
    file_path = SNAPSHOT_DIR / f"{target_date}.json"
    if file_path.exists():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return snapshot_to_df(data)
        except Exception as e:
            write_log(f"スナップショット読み込み失敗 ({target_date}): {e}")
    return None

def cleanup_old_snapshots(base_date):
    """3ヶ月より古いスナップショットファイルを自動削除する"""
    if not SNAPSHOT_DIR.exists():
        return

    cutoff_month = base_date.month - 3
    cutoff_year  = base_date.year
    if cutoff_month <= 0:
        cutoff_month += 12
        cutoff_year  -= 1
    max_day = calendar.monthrange(cutoff_year, cutoff_month)[1]
    cutoff  = datetime(cutoff_year, cutoff_month, min(base_date.day, max_day)).date()

    deleted = 0
    for f in SNAPSHOT_DIR.glob("*.json"):
        try:
            file_date = datetime.strptime(f.stem, "%Y-%m-%d").date()
            if file_date < cutoff:
                f.unlink()
                deleted += 1
        except ValueError:
            pass

    if deleted > 0:
        write_log(f"古いスナップショット削除: {deleted}件 ({cutoff}より前)")

def get_kpi_counts(df):
    """
    データフレームからKPI（UU数、成約数、終了数）を集計する
    UU数: 1 ■一次受付（ヒアリング）
    成約数: 1-50 ■成約
    終了数: 失注という文字が入ったステータス
    """
    if df.empty:
        return {"UU数": 0, "成約数": 0, "終了数": 0}
    
    statuses = df["最終ステータス"]
    
    uu_count = int((statuses == "1 ■一次受付（ヒアリング）").sum())
    contract_count = int((statuses == "1-50 ■成約").sum())
    lost_count = int(statuses.str.contains("失注", na=False).sum())
    
    return {
        "UU数": uu_count,
        "成約数": contract_count,
        "終了数": lost_count
    }

def update_spreadsheet_cells(df_current, target_dates):
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
    product_categories = [
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
        # 累計の集計期間（過去90日）
        start_window = d - timedelta(days=1)

        # ★修正：過去のスナップショットがあればそれを使う
        df_use = load_snapshot_for_date(d)
        if df_use is None:
            # 過去ファイルがない場合は、引数で渡された最新データ(df_current)を使用
            df_use = df_current
        else:
            write_log(f"  [{d}] 過去のスナップショットを使用して集計します")

        # 単日データ（その日のデータ）
        subset_day = df_use[df_use["日付_単体"] == d] if not df_use.empty else pd.DataFrame()
        
        # 累計データ（その日以前、かつ90日以内）
        subset_cum = df_use[(df_use["日付_単体"] <= d) & (df_use["日付_単体"] >= start_window)] if not df_use.empty else pd.DataFrame()

        for prod_name, start_col, prod_keyword in product_categories:
            target_col = start_col + (d.day - 1)

            # 商品タイプでフィルタ
            subset_day_prod = subset_day[subset_day["商品タイプ"].str.contains(prod_keyword, na=False)] if not subset_day.empty else pd.DataFrame()
            subset_cum_prod = subset_cum[subset_cum["商品タイプ"].str.contains(prod_keyword, na=False)] if not subset_cum.empty else pd.DataFrame()

            for config in URGENCY_CONFIG:
                keyword = config["keyword"]

                # --- 単日カウント ---
                counts_day = {"UU数": 0, "成約数": 0, "終了数": 0}
                if not subset_day_prod.empty:
                    # 緊急度でフィルタ
                    subset_cat = subset_day_prod[subset_day_prod["緊急度判定"].str.contains(keyword, na=False)]
                    counts_day = get_kpi_counts(subset_cat)

                # --- 累計カウント ---
                counts_cum = {"UU数": 0, "成約数": 0, "終了数": 0}
                if not subset_cum_prod.empty:
                    # 緊急度でフィルタ
                    subset_cat_cum = subset_cum_prod[subset_cum_prod["緊急度判定"].str.contains(keyword, na=False)]
                    counts_cum = get_kpi_counts(subset_cat_cum)

                # 単日書き込み
                for kpi_name, row_idx in config["rows_day"].items():
                    val = counts_day.get(kpi_name, 0)
                    cells_to_update_by_sheet[sheet_name].append(
                        gspread.Cell(row=row_idx, col=target_col, value=val if val > 0 else "-")
                    )

                # 累計書き込み
                for kpi_name, row_idx in config["rows_cum"].items():
                    val = counts_cum.get(kpi_name, 0)
                    cells_to_update_by_sheet[sheet_name].append(
                        gspread.Cell(row=row_idx, col=target_col, value=val if val > 0 else "-")
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
        # 自動モード：昨日を最終日として、過去30日分を対象にする
        # スナップショットがある日は正確な履歴データで、ない日は現在データで補完される
        end_date   = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=0)  # 30日分（昨日を含む）

    days = (end_date - start_date).days + 1
    target_dates = [start_date + timedelta(days=i) for i in range(days)]

    # 累計用データの取得開始日（集計開始日の3ヶ月前）
    cum_month = start_date.month - 3
    cum_year  = start_date.year
    if cum_month <= 0:
        cum_month += 12
        cum_year  -= 1
    max_day   = calendar.monthrange(cum_year, cum_month)[1]
    cum_start = datetime(cum_year, cum_month, min(start_date.day, max_day)).date()

    write_log(f"集計対象期間: {start_date} 〜 {end_date} ({days}日間)")
    write_log(f"データ取得範囲: {cum_start} 以降 (累計計算用)")

    # ── 古いスナップショットを削除（3ヶ月より前） ────────────────
    cleanup_old_snapshots(end_date)

    # ── 1. 問い合わせ管理DBからの取得 ────────────────────────
    write_log(">>> 問い合わせ管理DBの取得を開始します")
    df_raw = fetch_rakuraku_csv(DB_INQUIRY)

    df_current = pd.DataFrame()

    if not df_raw.empty:
        # 指定期間の最終日（通常は昨日/今日）の名前でスナップショット保存
        snapshot_data = save_snapshot(df_raw, end_date)

        # 最新データとして保持（スナップショットがない日のフォールバック用）
        df_current = snapshot_to_df(snapshot_data)

        write_log(f"スナップショット全件: {len(df_current)}件")
    else:
        write_log("データが取得できませんでした。")

    # ── 3. スプレッドシート更新 ──────────────────────────────
    update_spreadsheet_cells(df_current, target_dates)

    write_log("全処理完了。")

if __name__ == "__main__":
    main()
