import calendar
import pandas as pd
import requests
import io
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from pathlib import Path
import re
import os

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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
SPREADSHEET_KEY = "1AB5qDa4-k2kcgDTLe-agPfYNw69KlhdoHzukfavfFqQ"

# 3. 楽楽販売 API設定
RR_DOMAIN  = "hntobias.rakurakuhanbai.jp"
RR_ACCOUNT = "mspy4wa"
RR_TOKEN   = os.environ["RAKURAKU_TOKEN"]
RR_API_URL = f"https://{RR_DOMAIN}/{RR_ACCOUNT}/api/csvexport/version/v1"

# 4. 楽楽販売 DB設定 (問い合わせ管理)
DB_INQUIRY = {
    "dbSchemaId": "101181",
    "listId":     "101449",
    "searchIds": [
        "107226",
        "107228",
    ],
    "cols": {
        "id":            "記録ID",
        "date":          "新規問い合わせ日",
        "status":        "対応状況ステータス",
        "contract_date": "成約日時",
        "confirm_date":  "成約意思確認日時",
        "product_type":  "新規商品タイプ"
    }
}

# 4-3. 楽楽販売 DB設定 (成約管理)
DB_CONTRACT = {
    "dbSchemaId": "101185",
    "listId":     "101451",
    "searchIds": [
        "107230",
        "107239",
    ],
    "cols": {
        "id":            "手配番号",
        "inq_date":      "新規問い合わせ日",
        "contract_date": "成約日",
        "amount":        "（日程調整）請求金額（税込）",
        "product_type":  "商品タイプ"
    }
}

# 4-2. 1回のAPIリクエストで取得する最大件数
LIMIT_PER_REQUEST = 10000

# 5. スプレッドシートの書き込み位置設定
COL_START_ALL     = 6   # F列  (全体の開始列)
COL_START_KYUTOKI = 43  # AQ列 (給湯器の開始列)
COL_START_ECO     = 80  # CB列 (エコキュートの開始列)

# 6. ログファイル設定
EXCEL_FILE_NAME = "seiyaku_output_v2.xlsx"

# ★各ステータスと、スプレッドシートの「書き込み行番号」のマッピング（単日）
STATUS_ROW_MAP = {
    "1 ■一次受付（ヒアリング）": 46,
    "1-10 ■概算見積（写真フォーム案内なし）": 48,
    "1-11 ■概算見積（写真待ち）": 49,
    "1-20 ■確認中（施工可否、現調含む）": 50,
    "1-30 ■最終見積り済": 51,
    "1-40 ■現地調査（成約前）": 52,
    "当日成約": 54,
    "当日外成約": 55,
    "1-61-A ■失注：他社（価格）": 57,
    "1-61-B ■失注：他社（スピード）": 58,
    "1-61-C ■失注：交換見送り": 59,
    "1-61-D ■失注：その他・理由不明": 60,
    "1-61-E ■失注：終了": 61,
    "1-60 ■キャンセル（成約からキャンセル）": 62,
    "1-62 ■エリア外、施工・対応不可": 64,
    "その他全て": 66
}

# ★各ステータスと、スプレッドシートの「書き込み行番号」のマッピング（累計）
CUMULATIVE_ROW_MAP = {
    "1 ■一次受付（ヒアリング）": 8,
    "1-10 ■概算見積（写真フォーム案内なし）": 10,
    "1-11 ■概算見積（写真待ち）": 11,
    "1-20 ■確認中（施工可否、現調含む）": 12,
    "1-30 ■最終見積り済": 13,
    "1-40 ■現地調査（成約前）": 14,
    "当日成約": 16,
    "当日外成約": 17,
    "1-61-A ■失注：他社（価格）": 19,
    "1-61-B ■失注：他社（スピード）": 20,
    "1-61-C ■失注：交換見送り": 21,
    "1-61-D ■失注：その他・理由不明": 22,
    "1-61-E ■失注：終了": 23,
    "1-60 ■キャンセル（成約からキャンセル）": 24,
    "1-62 ■エリア外、施工・対応不可": 26,
    "その他全て": 28,
}

# ==============================================================================
# ■ 共通関数
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

def normalize_status(s):
    """
    指定されたステータスを正規化する。
    1. STATUS_ROW_MAP に定義済みのステータス（「その他全て」以外）はそのまま返す。
    2. 1-62 は前方一致で救済する。
    3. 「【E】エディオン紹介」「『FCへ紹介』」「その他（問い合わせ以外の物）」の3つのみ「その他全て」として扱う。
    4. それ以外は集計対象外として扱うため「_対象外_」を返す。
    """
    target_162_key = "1-62 ■エリア外、施工・対応不可"
    defined_keys = set(STATUS_ROW_MAP.keys()) - {"その他全て"}
    
    if s in defined_keys:
        return s
    
    if str(s).startswith("1-62"):
        return target_162_key
        
    other_targets = [
        "【E】エディオン紹介",
        "『FCへ紹介』",
        "その他（問い合わせ以外の物）"
    ]
    if s in other_targets:
        return "その他全て"
        
    return "_対象外_"

def fetch_rakuraku_csv(settings):
    """楽楽販売APIからデータを取得する。searchIds に設定された全IDを順に取得し、重複を除去して返す。"""
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

def save_snapshot(df_inq_raw, snapshot_date):
    """
    問い合わせ管理DBの全件スナップショットをJSONに保存する。
    ファイル名: snapshots/YYYY-MM-DD.json
    内容: {記録ID: {status, product_type, inq_date}}
    """
    SNAPSHOT_DIR.mkdir(exist_ok=True)

    cols = DB_INQUIRY["cols"]
    data = {}

    for _, row in df_inq_raw.iterrows():
        record_id = str(row.get(cols["id"], "")).strip()
        if not record_id or record_id == "nan":
            continue

        raw_status   = str(row.get(cols["status"], "")).strip()
        status       = normalize_status(raw_status)
        product_type = str(row.get(cols["product_type"], "")).strip()
        # 日付形式を YYYY-MM-DD に統一（スラッシュをハイフンに置換）
        inq_date     = str(row.get(cols["date"], ""))[:10].replace("/", "-")

        data[record_id] = {
            "status":       status,
            "product_type": product_type if product_type != "nan" else "",
            "inq_date":     inq_date,
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

    col_product = DB_INQUIRY["cols"]["product_type"]
    records = []

    for _, info in snapshot_data.items():
        inq_date_str = info.get("inq_date", "")
        inq_date = None
        try:
            if inq_date_str and len(inq_date_str) >= 10:
                inq_date = datetime.strptime(inq_date_str[:10], "%Y-%m-%d").date()
        except ValueError:
            pass

        # 累計フィルタ
        if cum_start is not None:
            if inq_date is None or inq_date < cum_start:
                continue

        records.append({
            "最終ステータス": info.get("status", "その他全て"),
            col_product:     info.get("product_type", ""),
            "日付_単体":      inq_date, # 日付フィルタ用に保持
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

# ==============================================================================
# ■ 成約管理DB処理（v1と同じ）
# ==============================================================================

def process_contract_raw_data(df):
    """成約管理DBのデータを加工して、ステータスと商品タイプを決定する"""
    cols = DB_CONTRACT["cols"]

    # ---------------------------------------------------------
    # ★列名が見つからない場合の強力な補正ロジック
    # ---------------------------------------------------------
    target_amount_col = cols["amount"]
    if target_amount_col not in df.columns:
        write_log(f"警告: 設定された列名 '{target_amount_col}' がCSVに見つかりません。自動検索します...")

        candidates = [c for c in df.columns if "請求" in str(c) and "金額" in str(c)]

        if candidates:
            new_col = candidates[0]
            write_log(f"  → 類似した列名 '{new_col}' を発見しました。これを使用します。")
            cols["amount"] = new_col

        elif len(df.columns) >= 12:
            l_col_name = df.columns[11]
            write_log(f"  → 類似列も見つかりませんでしたが、L列にある '{l_col_name}' を代わりに使用します。")
            cols["amount"] = l_col_name

        else:
            write_log(f"  → 解決できませんでした。現在の列一覧: {list(df.columns)}")
    # ---------------------------------------------------------

    df["成約日_dt"] = pd.to_datetime(df[cols["contract_date"]], errors="coerce")
    df = df.dropna(subset=["成約日_dt"])
    df["日付_単体"] = df["成約日_dt"].dt.date

    def debug_amount_info(row):
        col_name = cols["amount"]
        if col_name not in row:
            return f"エラー: 列「{col_name}」が見つかりません"
        raw_val   = str(row.get(col_name, ""))
        clean_val = re.sub(r'[^\d.]', '', raw_val)
        return f"元値='{raw_val}' -> 変換後='{clean_val}'"

    df["DEBUG_金額判定"] = df.apply(debug_amount_info, axis=1)

    def get_product_type(row):
        ptype = str(row.get(cols["product_type"], "")).strip()
        if ptype and ptype != "nan":
            return ptype
        try:
            val_str   = str(row.get(cols["amount"], "0"))
            val_clean = re.sub(r'[^\d.]', '', val_str)
            val       = float(val_clean) if val_clean else 0
        except:
            val = 0
        if 100000 <= val < 300000:
            return "給湯器"
        elif val >= 300000:
            return "エコキュート"
        return None

    df["判定商品タイプ"] = df.apply(get_product_type, axis=1)

    def get_status(row):
        c_date = str(row.get(cols["contract_date"], ""))[:10]
        i_date = str(row.get(cols["inq_date"], ""))[:10]
        if c_date and i_date and c_date == i_date:
            return "当日成約"
        else:
            return "当日外成約"

    df["最終ステータス"] = df.apply(get_status, axis=1)
    df["成約判定区分"]   = df["最終ステータス"]
    return df

# ==============================================================================
# ■ スプレッドシート更新
# ==============================================================================

def update_spreadsheet_cells(df_inq_current, df_cont_target, df_cont_cum, target_dates):
    """
    スナップショットデータを使ってスプレッドシートを更新する。

    単日 (STATUS_ROW_MAP):
      - 問い合わせDB: その日のスナップショット（なければ最新）のステータス分布
      - 成約DB:       成約日 = 対象日 の件数（v1と同じ）

    累計 (CUMULATIVE_ROW_MAP):
      - 問い合わせDB: その日のスナップショット（なければ最新）のうち問い合わせ日が当月の案件
      - 成約DB:       成約日が当月内で、かつ対象日以前の案件
    """
    write_log("スプレッドシートを更新中...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    try:
        creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds    = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client   = gspread.authorize(creds)
        workbook = client.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        write_log(f"スプレッドシート接続エラー: {e}")
        return

    cells_to_update_by_sheet = {}
    sheet_cache = {}

    # キーワードが None の場合は商品タイプによるフィルタなし（全体集計）
    product_categories = [
        ("全体",       COL_START_ALL,     None),
        ("給湯器",     COL_START_KYUTOKI, "給湯器"),
        ("エコキュート", COL_START_ECO,     "エコ"),
    ]
    col_product_inq = DB_INQUIRY["cols"]["product_type"]

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
        # 累計の集計期間（当月1日から当日まで）
        start_window = d.replace(day=1)

        # 1. 成約DB（成約日ベース）
        subset_day_cont = df_cont_target[df_cont_target["日付_単体"] == d] if not df_cont_target.empty else pd.DataFrame()
        subset_cum_cont = df_cont_cum[(df_cont_cum["日付_単体"] <= d) & (df_cont_cum["日付_単体"] >= start_window)] if not df_cont_cum.empty else pd.DataFrame()

        # 2. 問い合わせDB（問い合わせ日ベース）★修正：過去のスナップショットがあればそれを使う
        df_inq_use = load_snapshot_for_date(d)
        if df_inq_use is None:
            # 過去ファイルがない場合は、引数で渡された最新データ(df_inq_current)を使用（従来通りの挙動）
            df_inq_use = df_inq_current
        else:
            write_log(f"  [{d}] 過去のスナップショットを使用して集計します")

        subset_day_inq = df_inq_use[df_inq_use["日付_単体"] == d] if not df_inq_use.empty else pd.DataFrame()
        subset_cum_inq = df_inq_use[(df_inq_use["日付_単体"] <= d) & (df_inq_use["日付_単体"] >= start_window)] if not df_inq_use.empty else pd.DataFrame()

        for _cat_name, start_col, keyword in product_categories:
            target_col = start_col + (d.day - 1)

            # --- 単日カウント ---
            counts_day = pd.Series(dtype=int)
            if not subset_day_inq.empty:
                if keyword is None:
                    subset_cat = subset_day_inq
                else:
                    subset_cat = subset_day_inq[subset_day_inq[col_product_inq].astype(str).str.contains(keyword, na=False)]
                counts_day = subset_cat.groupby("最終ステータス").size()

            if not subset_day_cont.empty:
                if keyword is None:
                    subset_cat_cont = subset_day_cont
                else:
                    subset_cat_cont = subset_day_cont[subset_day_cont["判定商品タイプ"].astype(str).str.contains(keyword, na=False)]
                counts_day_cont = subset_cat_cont.groupby("最終ステータス").size()
                counts_day      = counts_day.add(counts_day_cont, fill_value=0)

            # --- 累計カウント ---
            counts_cum = pd.Series(dtype=int)
            if not subset_cum_inq.empty:
                if keyword is None:
                    subset_cat_cum = subset_cum_inq
                else:
                    subset_cat_cum = subset_cum_inq[subset_cum_inq[col_product_inq].astype(str).str.contains(keyword, na=False)]
                counts_cum = subset_cat_cum.groupby("最終ステータス").size()

            if not subset_cum_cont.empty:
                if keyword is None:
                    subset_cat_cum_cont = subset_cum_cont
                else:
                    subset_cat_cum_cont = subset_cum_cont[subset_cum_cont["判定商品タイプ"].astype(str).str.contains(keyword, na=False)]
                counts_cum_cont = subset_cat_cum_cont.groupby("最終ステータス").size()
                counts_cum      = counts_cum.add(counts_cum_cont, fill_value=0)

            # 単日をSTATUS_ROW_MAPの行に書き込む
            for status_name, row_idx in STATUS_ROW_MAP.items():
                count_val = int(counts_day.get(status_name, 0))
                cells_to_update_by_sheet[sheet_name].append(
                    gspread.Cell(row=row_idx, col=target_col, value=count_val if count_val > 0 else "")
                )

            # 累計をCUMULATIVE_ROW_MAPの行に書き込む
            for status_name, row_idx in CUMULATIVE_ROW_MAP.items():
                count_val = int(counts_cum.get(status_name, 0))
                cells_to_update_by_sheet[sheet_name].append(
                    gspread.Cell(row=row_idx, col=target_col, value=count_val if count_val > 0 else "")
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
        # 手動指定モード
        start_date = datetime.strptime(TARGET_DATE_START, "%Y/%m/%d").date()
        end_date   = datetime.strptime(TARGET_DATE_END,   "%Y/%m/%d").date()
    else:
        # 自動モード：過去3日間（3日前〜前日）を集計対象とする
        end_date   = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)
        
    days = (end_date - start_date).days + 1

    # --- 単日としてスプレッドシートに書き込む対象日 ---
    target_dates = [start_date + timedelta(days=i) for i in range(days)]

    # 累計用データの取得開始日（集計開始日の当月1日）
    cum_start = start_date.replace(day=1)

    write_log(f"集計対象期間: {start_date} 〜 {end_date} ({days}日間)")
    write_log(f"データ取得範囲: {cum_start} 以降 (累計計算用)")

    # ── 古いスナップショットを削除（3ヶ月より前） ────────────────
    cleanup_old_snapshots(end_date)

    # ── 1. 問い合わせ管理DBの取得・スナップショット保存 ──────────
    write_log(">>> 問い合わせ管理DBの取得を開始します")
    df_inq_raw = fetch_rakuraku_csv(DB_INQUIRY)

    df_inq_current = pd.DataFrame()

    if not df_inq_raw.empty:
        # 指定期間の最終日（通常は昨日/今日）の名前でスナップショット保存
        snapshot_data = save_snapshot(df_inq_raw, end_date)

        # 最新データとして保持（スナップショットがない日のフォールバック用）
        df_inq_current = snapshot_to_df(snapshot_data)

        write_log(f"スナップショット全件: {len(df_inq_current)}件")

    # ── 2. 成約管理DBの取得・加工（v1と同じ） ───────────────────
    write_log(">>> 成約管理DBの取得を開始します")
    df_cont_raw = fetch_rakuraku_csv(DB_CONTRACT)

    df_cont_target = pd.DataFrame()
    df_cont_cum    = pd.DataFrame()

    if not df_cont_raw.empty:
        df_cont_processed = process_contract_raw_data(df_cont_raw)

        # 単日用フィルタ（指定期間内）
        mask_target    = (df_cont_processed["日付_単体"] >= start_date) & (df_cont_processed["日付_単体"] <= end_date)
        df_cont_target = df_cont_processed[mask_target].copy()

        # 累計用フィルタ（データ取得範囲以降 〜 指定期間終了まで）
        mask_cum    = (df_cont_processed["日付_単体"] >= cum_start) & (df_cont_processed["日付_単体"] <= end_date)
        df_cont_cum = df_cont_processed[mask_cum].copy()

        write_log(f"成約データ(単日): {len(df_cont_target)}件, 成約データ(累計): {len(df_cont_cum)}件")

    # ── Excelファイルへの出力 ─────────────────────────────────
    # (出力不要とのことでコメントアウト)
    # try:
    #     excel_path = Path(__file__).parent / EXCEL_FILE_NAME
    #     df_export  = pd.concat([df_inq_current, df_cont_target], ignore_index=True)
    #     df_export.to_excel(excel_path, index=False)
    #     write_log(f"Excelファイルを出力しました: {EXCEL_FILE_NAME}")
    # except Exception as e:
    #     write_log(f"Excel出力エラー: {e}")

    # ── スプレッドシート更新 ──────────────────────────────────
    update_spreadsheet_cells(df_inq_current, df_cont_target, df_cont_cum, target_dates)

    write_log("全処理完了。")

if __name__ == "__main__":
    main()
