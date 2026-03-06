"""
【実行内容】
楽楽販売の問い合わせ管理DBからデータを取得し、Gemini API (AI) を用いて「UU」「修理」「止」「その他電話」などに分類します。
分類結果を集計し、Googleスプレッドシートの月別シートに日別・カテゴリ別の件数を書き込みます。
"""
import pandas as pd
import requests
import io
import json
import gspread
import google.generativeai as genai
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from pathlib import Path
import time
import os

# ==============================================================================
# ■ 設定エリア
# ==============================================================================

# 1. 実行対象の「基準日」指定 (YYYY/MM/DD 形式)
MANUAL_TARGET_DATE = ""  
DAYS_TO_SYNC = 1

# 2. Gemini APIキー
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL_NAME = "gemini-3-flash-preview"

# 3. Googleスプレッドシート設定
SPREADSHEET_KEY = "1ETKVEAHVJV5NZ9bE4TTmcgsAI3FoNq1_V0xBCoYdrt0"

# 4. 楽楽販売 API設定
RR_DOMAIN  = "hntobias.rakurakuhanbai.jp"
RR_ACCOUNT = "mspy4wa"
RR_TOKEN   = os.environ["RAKURAKU_TOKEN"]
RR_API_URL = f"https://{RR_DOMAIN}/{RR_ACCOUNT}/api/csvexport/version/v1"

# 5. 楽楽販売 DB設定 (問い合わせ管理)
DB_INQUIRY = {
    "dbSchemaId": "101181",
    "listId":     "101446", 
    "searchIds":  ["107226", "107228"],
    "cols": {
        "id":       "記録ID",      
        "date":     "登録日",      
        "new_type": "新規商品タイプ",
        "status":   "対応状況ステータス",
        "history":  "対応履歴"     
    }
}

# 6. スプレッドシートの書き込み位置設定
COL_START = 3

ROW_MAP = {
    "給湯器（UU）": 6,
    "給湯器（修理）": 7,
    "給湯器（止）": 8,
    "給湯器（その他電話）": 9,
    "エコキュート（UU）": 10,
    "エコキュート（修理）": 11,
    "エコキュート（止）": 12,
    "エコキュート（その他電話）": 13,
    "コンロ（UU）": 14,
    "その他商品（UU）": 15,
    "不明": 16
}

# ==============================================================================
# ■ AIプロンプト (判定ルール)
# ==============================================================================

SYSTEM_PROMPT = """
あなたは住宅設備の問い合わせデータを分類する熟練オペレーターです。
以下の「判定ルール」を厳守し、入力されたデータをJSON形式で分類してください。

【出力JSON形式】
必ず以下の形式のJSON配列で出力してください。（Markdownタグ ```json は不要）
[
  {"id": "記録ID", "category": "給湯器", "sub_category": "UU"},
  ...
]

【判定ルール 1: 商材カテゴリ (category)】
1. 「新規商品タイプ」または「対応履歴」から判断してください。
2. 以下のいずれかを出力してください。
   - "給湯器" (ガス給湯器, 石油給湯器, GT-, RUF- など)
   - "エコキュート" (エコキュート, 電気温水器, ヒートポンプ, SRT- など)
   - "コンロ" (ガスコンロ, IH, N3W, PD- など)
   - "その他商品" (レンジフード, 食洗機, トイレなど)
   - どれにも当てはまらない場合は "不明"

【判定ルール 2: 問い合わせ種別 (sub_category)】
以下の優先順位で問い合わせの内容を分類してください。

1. **"止" (緊急の停止)**
   - 履歴に「お湯が出ない」「点火しない」「水しか出ない」「エラーコードが出ている」「完全に止まった」などの記載がある場合。

2. **"修理" (修理の依頼)**
   - 履歴に「修理してほしい」「直してほしい」などの記載がある場合。ステータスが「修理対応依頼」の場合も含む。

3. **"その他電話" (営業、クレーム、間違い電話など)**
   - 履歴やステータスに「営業」「勧誘」「間違い電話」「クレーム」「重複」「不可」「エリア外」などの記載がある場合。

4. **"UU" (純粋な新規問い合わせ・見積・交換)**
   - 上記1〜3に当てはまらない、通常の見積依頼や交換工事の依頼。
   - 履歴に「見積」「交換」「新設商品」「【交換工事に含まれるもの】」などがある場合。
   - ステータスが「1」で始まるもの（1-10, 1-50成約, 1-61失注など）も原則としてUU。
"""

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
    combined_df = combined_df.drop_duplicates(subset=[settings["cols"]["id"]]) 
    return combined_df

def load_and_filter_data(df, target_dates):
    cols = DB_INQUIRY["cols"]
    df["日付_dt"] = pd.to_datetime(df[cols["date"]], errors="coerce")
    df = df.dropna(subset=["日付_dt"])
    
    start_date = target_dates[0]
    end_date = target_dates[-1]
    
    mask = (df["日付_dt"].dt.date >= start_date) & (df["日付_dt"].dt.date <= end_date)
    df_filtered = df[mask].copy()

    if df_filtered.empty:
        write_log(f"指定期間 ({start_date} 〜 {end_date}) のデータはありませんでした。")
        return pd.DataFrame()

    df_filtered["日付_単体"] = df_filtered["日付_dt"].dt.date
    return df_filtered

def classify_all_with_gemini(df):
    if df.empty: return {}
    genai.configure(api_key=GEMINI_API_KEY)
    
    generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
    model = genai.GenerativeModel(GEMINI_MODEL_NAME, system_instruction=SYSTEM_PROMPT, generation_config=generation_config)
    
    all_records = []
    cols = DB_INQUIRY["cols"]
    for _, row in df.iterrows():
        rec_text = f"【ID】{row[cols['id']]}\n【ステータス】{row.get(cols['status'], '')}\n【タイプ】{row.get(cols['new_type'], '')}\n【履歴】{str(row.get(cols['history'], ''))[:1000]}"
        all_records.append({"id": str(row[cols['id']]), "text": rec_text})
    
    BATCH_SIZE = 20 
    results_map = {}
    total_batches = (len(all_records) + BATCH_SIZE - 1) // BATCH_SIZE
    write_log(f"Geminiへリクエスト送信開始 (全{len(all_records)}件)")

    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i + BATCH_SIZE]
        write_log(f"  - Batch {i // BATCH_SIZE + 1}/{total_batches} 処理中...")
        prompt = json.dumps(batch, ensure_ascii=False)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = model.generate_content(prompt)
                batch_res = json.loads(response.text)
                for item in batch_res: results_map[item["id"]] = item
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    write_log(f"  API Failed: {e}")

    return results_map

def map_final_label(category, sub_category):
    c = str(category).strip()
    s = str(sub_category).strip()

    if c == "給湯器":
        if s == "UU": return "給湯器（UU）"
        elif s == "修理": return "給湯器（修理）"
        elif s == "止": return "給湯器（止）"
        else: return "給湯器（その他電話）"
    elif c == "エコキュート":
        if s == "UU": return "エコキュート（UU）"
        elif s == "修理": return "エコキュート（修理）"
        elif s == "止": return "エコキュート（止）"
        else: return "エコキュート（その他電話）"
    elif c == "コンロ" and s == "UU": 
        return "コンロ（UU）"
    elif c == "その他商品" and s == "UU": 
        return "その他商品（UU）"
    
    return "不明"

def update_spreadsheet_cells(df_target, target_dates):
    write_log("スプレッドシートを更新中...")
    
    # 環境変数からサービスアカウントJSONを読み込む
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        workbook = client.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        write_log(f"スプレッドシート接続エラー: {e}")
        return

    cells_to_update_by_sheet = {}
    sheet_cache = {}

    for d in target_dates:
        sheet_name = f"{d.month}月"
        
        if sheet_name not in sheet_cache:
            try:
                sheet_cache[sheet_name] = workbook.worksheet(sheet_name)
                cells_to_update_by_sheet[sheet_name] = []
            except Exception as e:
                sheet_cache[sheet_name] = None
                
        if not sheet_cache[sheet_name]:
            continue
            
        target_day = d.day
        target_col = COL_START + (target_day - 1)
        
        subset = df_target[df_target["日付_単体"] == d] if not df_target.empty else pd.DataFrame()
        
        if not subset.empty:
            counts = subset.groupby("最終ラベル").size()
        else:
            counts = {}

        for label_name, row_idx in ROW_MAP.items():
            count_val = int(counts.get(label_name, 0))
            display_val = count_val if count_val > 0 else ""
            cells_to_update_by_sheet[sheet_name].append(gspread.Cell(row=row_idx, col=target_col, value=display_val))

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

    df_raw = fetch_rakuraku_csv(DB_INQUIRY)
    if df_raw.empty:
        write_log("データが取得できませんでした。処理を終了します。")
        return

    df_target = load_and_filter_data(df_raw, target_dates)
    if df_target.empty:
        return

    ai_results = classify_all_with_gemini(df_target)

    final_labels = []
    cols = DB_INQUIRY["cols"]
    for _, row in df_target.iterrows():
        rid = str(row[cols['id']])
        res = ai_results.get(rid, {})
        cat = res.get("category", "不明")
        sub = res.get("sub_category", "その他")
        
        label = map_final_label(cat, sub)
        final_labels.append(label)

    df_target["最終ラベル"] = final_labels

    log_filename = f"log_問い合わせ集計_{target_dates[0].strftime('%Y%m%d')}_{target_dates[-1].strftime('%Y%m%d')}.csv"
    df_target.to_csv(log_filename, index=False, encoding="utf-8-sig")
    write_log(f"ログを保存しました: {log_filename}")

    update_spreadsheet_cells(df_target, target_dates)
    
    write_log("全処理完了。")

if __name__ == "__main__":
    main()
