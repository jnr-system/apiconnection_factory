"""
【実行内容】
楽楽販売APIから問い合わせデータを取得し、Gemini API (AI) を使用して問い合わせ内容（UU、修理、その他など）や
商材カテゴリを自動分類します。分類結果に基づき、Googleスプレッドシートの日別シート（UU件数）および
全体シート（詳細分類集計）を更新します。
"""
import pandas as pd
import requests
import io
import json
import gspread
from google import genai
from google.genai import types
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from pathlib import Path
import time
import re
import os

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 本番環境では python-dotenv がなくても動作する

# ==============================================================================
# ■ 設定エリア (ここを変更してください)
# ==============================================================================


# 1. Gemini APIキー
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL_NAME = "gemini-3-flash-preview"

# 2. Googleスプレッドシート設定
SPREADSHEET_KEY = "1N606OaDTizfMskMh09INPPfTOk2xIVCuRFgl1eUjKEY"

# 3. 楽楽販売 API設定
RR_DOMAIN   = "hntobias.rakurakuhanbai.jp"
RR_ACCOUNT  = "mspy4wa"
RR_TOKEN    = os.environ["RAKURAKU_TOKEN"]
RR_API_URL  = f"https://{RR_DOMAIN}/{RR_ACCOUNT}/api/csvexport/version/v1"

# 4. 楽楽販売 DB設定
DB_INQUIRY = {
    "dbSchemaId": "101181",
    "listId":     "101446", 
    "searchId":   "107213", 
    "cols": {
        "id":       "記録ID",      
        "date":     "登録日",      
        "pref":     "都道府県名",  
        "address":  "メール差し込み用：住所",
        "new_type": "新規商品タイプ",
        "status":   "対応状況ステータス",
        "history":  "対応履歴"     
    }
}

# 5. スプレッドシートの書き込み位置
COL_START_GAS = 3   # C列
COL_START_ECO = 35  # AI列
START_ROW = 6
UNKNOWN_ROW = 53
PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
]

# ==============================================================================
# ★AIへの指示書 (System Prompt) - 特例絶対優先版
# ==============================================================================

SYSTEM_PROMPT = """
あなたは住宅設備の問い合わせデータを分類する熟練オペレーターです。
以下の「判定ルール」を厳守し、入力されたデータをJSON形式で分類してください。

【入力データ項目】
- ID
- ステータス (対応状況)
- 新規商品タイプ (最優先の判断材料)
- 対応履歴 (フリーテキスト)
- 住所

【判定ルール 1: ★最優先・特例判定★ (Absolute Priority)】
他のあらゆるルール（除外ルール含む）に優先して、以下の文言がある場合は必ず「UU（新規）」としてください。
たとえステータスが「重複」や「クレーム」であっても、以下の文言があれば「UU」が勝ちます。
**ただし、「対応履歴」に「不可」や「エリア外」という文言が含まれる場合は、本ルールの対象外（UUとしない）としてください。**

- 「対応履歴」の中に以下のいずれかのフレーズが含まれている場合
  - **"【交換工事に含まれるもの】"**
  - **"新設商品"**
- -> **sub_category="UU"** と判定してください。
  （categoryは文脈から「給湯器」「エコキュート」などを判定）

【判定ルール 2: 除外・対象外判定 (Exclusion)】
※ルール1に当てはまらなかったデータのうち、ステータスまたは履歴に以下の言葉が含まれる場合は集計対象外としてください。
- **「不可」「エリア外」**: これらが含まれる場合は、category="その他", sub_category="その他" としてください。
- **「重複」**: これが含まれる場合は必ず category="その他", sub_category="その他" としてください。
- **「クレーム」「削除」「テスト」**: これらも同様に対象外です。

【判定ルール 3: 商材カテゴリ (Category)】
優先順位の高い順に判定してください。

1. 「新規商品タイプ」に入力がある場合
   - "給湯器"を含む -> "給湯器"
   - "エコキュート"または"電気温水器"を含む -> "エコキュート"
   - "コンロ"を含む -> "コンロ"
   - "レンジフード"または"食洗機"を含む -> "その他商品"

2. 「対応履歴」から以下の品番・キーワードを探す (タイプが空の場合)
   - **エコキュート**:
     - 品番: SRT-, HE-, BE-, EQ, EQN, EQX, TU, BHP-, HWH-, CHP-, CTU-, EHP-
     - 単語: エコキュート, 電気温水器, ヒートポンプ
   - **給湯器**:
     - 品番: GT-, GTH-, GQ-, GRQ-, OTQ-, OTX-, OQB-, OX-, OH-, RUF-, RVD-, RUX-, RFS-, FH-, PH-, GX-, GH-, GFK-, KIB-, IB-
     - 単語: エコジョーズ, 給湯器, 風呂釜
   - **コンロ**:
     - 単語: コンロ, ビルトイン, IH, PD-, RHS
   - **その他商品**:
     - 単語: レンジフード, 食洗機, 水栓, トイレ

※どれにも当てはまらない場合は "不明" とする。

【判定ルール 4: サブカテゴリ (Sub Category) - ★ステータス判定★】
※ルール1（特例）に該当しない場合のみ、以下の順序で判定してください。

1. **新規問い合わせ (UU) とするステータス**
   - **「1」で始まる全てのステータス**
     - 1-00, 1-10, 1-50 (成約) はもちろん、
     - **1-60 (キャンセル), 1-61 (失注) も全て「UU」**として扱ってください。（※エリア外は除外）
   - **「【E】」** (エディオン紹介) で始まるステータス
   - -> **sub_category="UU"**

2. **修理 (Repair) とするステータス**
   - **「修理対応依頼」** が含まれるもの
   - -> **sub_category="修理"**

3. **その他 (Other) とするステータス**
   - 「重複」「再工事」「残工事」「クレーム」「連絡禁止」「その他（問い合わせ以外の物）」
   - -> **sub_category="その他電話"**
   - **★特例: ただし「その他（問い合わせ以外の物）」のステータスでも、対応履歴に「見積」というワードが含まれる場合は sub_category="UU" としてください。**

4. **キーワードによる判定 (ステータスが空欄の場合のみ)**
   - "お湯が出ない", "点火しない", "水しか" -> "止" (緊急)
   - "修理", "故障", "エラー" -> "修理"
   - "見積", "交換" -> "UU"
   - "営業", "間違い" -> "その他電話"

【判定ルール 5: 都道府県 (Prefecture)】
- 住所または履歴から都道府県名を特定してください。
- 特定できない場合は "不明" としてください。

【出力JSON形式】
[
  {"id": "...", "category": "給湯器", "sub_category": "UU", "prefecture": "東京都"},
  ...
]
"""

# ==============================================================================
# 関数定義
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
    payload = {
        "dbSchemaId": settings["dbSchemaId"],
        "listId":     settings["listId"],
        "searchId":   settings["searchId"],
        "limit":      10000 
    }
    write_log(f"データ取得中... (SchemaID: {settings['dbSchemaId']})")
    try:
        res = requests.post(RR_API_URL, headers=headers, json=payload, timeout=60)
        if res.status_code != 200:
            write_log(f"APIエラー: {res.status_code}")
            return pd.DataFrame()
        try: content = res.content.decode("cp932")
        except: content = res.content.decode("utf-8", errors="ignore")
        return pd.read_csv(io.StringIO(content), dtype=str)
    except Exception as e:
        write_log(f"接続エラー: {e}")
        return pd.DataFrame()

def get_prefecture_simple(row):
    pref_col = DB_INQUIRY["cols"]["pref"]
    if pref_col in row and pd.notna(row[pref_col]):
        pref = str(row[pref_col]).strip()
        if pref: return pref
    addr_col = DB_INQUIRY["cols"]["address"]
    address = str(row.get(addr_col, "")).strip()
    for p in PREFECTURES:
        if address.startswith(p): return p
    return "不明"

def load_and_clean_data(start_date, end_date):
    df = fetch_rakuraku_csv(DB_INQUIRY)
    if df.empty: return pd.DataFrame()
    
    id_col = DB_INQUIRY["cols"]["id"]
    date_col = DB_INQUIRY["cols"]["date"]
    hist_col = DB_INQUIRY["cols"]["history"]
    type_col = DB_INQUIRY["cols"]["new_type"]
    stat_col = DB_INQUIRY["cols"]["status"]
    addr_col = DB_INQUIRY["cols"]["address"]
    
    if id_col not in df.columns: 
        write_log("エラー: ID列が見つかりません")
        return pd.DataFrame()

    df['登録日'] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=['登録日']) 

    s_ts = start_date.replace(hour=0, minute=0, second=0)
    e_ts = end_date.replace(hour=23, minute=59, second=59)
    mask = (df['登録日'] >= s_ts) & (df['登録日'] <= e_ts)
    df = df[mask].copy()

    write_log(f"期間データ: {len(df)}件")
    if df.empty: return pd.DataFrame()

    df['集計日'] = df['登録日'].dt.date
    df['Python_Pref'] = df.apply(get_prefecture_simple, axis=1)
    
    hist_series = df.groupby(id_col)[hist_col].apply(lambda x: " ".join([str(v) for v in x if pd.notna(v)]))
    cols_to_keep = [id_col, '集計日', 'Python_Pref']
    for c in [type_col, stat_col, addr_col]:
        if c in df.columns: cols_to_keep.append(c)
    
    df_unique = df[cols_to_keep].groupby(id_col).first().reset_index()
    grouped = pd.merge(df_unique, hist_series, on=id_col, how='left')
    
    rename_map = {id_col: '記録ID', hist_col: '対応履歴', type_col: '新規商品タイプ', stat_col: 'ステータス', addr_col: '住所'}
    rename_map = {k: v for k, v in rename_map.items() if k in grouped.columns}
    grouped = grouped.rename(columns=rename_map)
    
    if 'ステータス' not in grouped.columns: grouped['ステータス'] = ""
    return grouped

def classify_all_with_gemini(df):
    """Gemini APIで一括分類"""
    if df.empty: return {}
    client = genai.Client(api_key=GEMINI_API_KEY)

    generate_config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        response_mime_type="application/json"
    )

    all_records = []
    for _, row in df.iterrows():
        rec_text = f"【ID】{row['記録ID']}\n【ステータス】{row.get('ステータス', '')}\n【新規商品タイプ】{row.get('新規商品タイプ', '')}\n【住所】{row.get('住所', '')}\n【履歴】{str(row['対応履歴'])[:1000]}"
        all_records.append({"id": str(row['記録ID']), "text": rec_text})

    BATCH_SIZE = 20
    results_map = {}
    total_batches = (len(all_records) + BATCH_SIZE - 1) // BATCH_SIZE
    write_log(f"Geminiへリクエスト送信開始 (全{len(all_records)}件)")

    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        write_log(f"  - Batch {batch_num}/{total_batches} 処理中...")

        prompt = json.dumps(batch, ensure_ascii=False)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL_NAME,
                    contents=prompt,
                    config=generate_config
                )
                batch_res = json.loads(response.text)
                for item in batch_res: results_map[item["id"]] = item
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    write_log(f"  Retry... {e}")
                    time.sleep(2)
                else:
                    write_log(f"  Failed: {e}")

    return results_map

# ==============================================================================
# 更新処理
# ==============================================================================

def update_spreadsheet_daily(counts_dict, target_day_int, sheet_name):
    """日別シート更新 (UUのみ)"""
    write_log(f"日別シート '{sheet_name}' (Day={target_day_int}) 更新...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_KEY).worksheet(sheet_name)
    except Exception as e:
        write_log(f"シートエラー: {e}")
        return

    col_gas = COL_START_GAS + (target_day_int - 1)
    col_eco = COL_START_ECO + (target_day_int - 1)
    
    cells = []
    for i, pref in enumerate(PREFECTURES):
        r = START_ROW + i
        v_gas = counts_dict.get((target_day_int, pref, "給湯器"), 0) or "-"
        cells.append(gspread.Cell(row=r, col=col_gas, value=v_gas))
        v_eco = counts_dict.get((target_day_int, pref, "エコキュート"), 0) or "-"
        cells.append(gspread.Cell(row=r, col=col_eco, value=v_eco))

    v_gas_unk = counts_dict.get((target_day_int, "不明", "給湯器"), 0) or "-"
    cells.append(gspread.Cell(row=UNKNOWN_ROW, col=col_gas, value=v_gas_unk))
    v_eco_unk = counts_dict.get((target_day_int, "不明", "エコキュート"), 0) or "-"
    cells.append(gspread.Cell(row=UNKNOWN_ROW, col=col_eco, value=v_eco_unk))
    
    if cells: sheet.update_cells(cells)

def update_spreadsheet_total(total_counts, target_month, sheet_name="全体"):
    """全体シート更新 (累積加算 & 月別横展開)"""
    write_log(f"全体シート '{sheet_name}' ({target_month}月) 更新...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_KEY).worksheet(sheet_name)
    except Exception as e:
        write_log(f"シートエラー: {e}")
        return

    row_map = {
        "給湯器（UU）": 6,
        "給湯器（修理）": 7,
        "給湯器（止）": 8,
        "給湯器（その他電話）": 9,
        "エコ　（UU）": 10,
        "エコ　（修理）": 11,
        "エコ　（止）": 12,
        "エコキュート（その他電話）": 13,
        "コンロ（UU）": 14,
        "その他商品（UU）": 15,
        "不明": 16
    }
    
    # 列の計算: 1月=C(3), 2月=D(4)...
    target_col = target_month + 2
    
    # 対象範囲のセルを取得 (6行目〜16行目, target_col列)
    try:
        cell_list = sheet.range(6, target_col, 16, target_col)
    except Exception as e:
        write_log(f"セル取得エラー: {e}")
        return

    cells_to_update = []
    row_to_label = {v: k for k, v in row_map.items()}
    
    for cell in cell_list:
        label = row_to_label.get(cell.row)
        if label and label in total_counts:
            add_val = total_counts[label]
            if add_val > 0:
                try:
                    current_val = int(cell.value) if cell.value and cell.value.strip().isdigit() else 0
                except ValueError:
                    current_val = 0
                
                cell.value = current_val + add_val
                cells_to_update.append(cell)
        
    if cells_to_update:
        sheet.update_cells(cells_to_update)
        write_log(f"{len(cells_to_update)}箇所のセルを更新(加算)しました。")
    else:
        write_log("更新対象のデータはありませんでした。")

# ==============================================================================
# メイン
# ==============================================================================

def main():
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday  = today - timedelta(days=1)
    start_date = yesterday
    end_date   = yesterday

    write_log("=== 自動分類処理を開始します ===")

    write_log(f"処理実行: {start_date.strftime('%Y/%m/%d')} - {end_date.strftime('%Y/%m/%d')}")

    # 1. データ取得
    df = load_and_clean_data(start_date, end_date)
    if df.empty: return

    # 2. Gemini分類
    gemini_results = classify_all_with_gemini(df)
    
    results_list = []
    for _, row in df.iterrows():
        rid = str(row['記録ID'])
        res = gemini_results.get(rid, {})
        
        cat = res.get("category", "不明")
        sub = res.get("sub_category", "その他")
        g_pref = res.get("prefecture", "")
        
        final_pref = g_pref if g_pref in PREFECTURES else (row['Python_Pref'] if row['Python_Pref'] in PREFECTURES else "不明")
        
        results_list.append({
            "id": rid,
            "date": row['集計日'], 
            "cat": cat, 
            "sub": sub, 
            "pref": final_pref,
            # ログ用
            "status": row.get('ステータス', ''),
            "new_type": row.get('新規商品タイプ', ''),
            "history": row.get('対応履歴', ''),
            "address": row.get('住所', '')
        })
    
    df_res = pd.DataFrame(results_list)

    # 3. 集計 & 更新 (日付ごとにループ)
    delta = end_date - start_date
    for i in range(delta.days + 1):
        current_date = (start_date + timedelta(days=i)).date()
        target_day_int = current_date.day
        target_month_str = f"{current_date.month}月"
        
        df_day = df_res[df_res['date'] == current_date]
        
        # (A) 日別 (UUのみ)
        daily_counts = {}
        for _, row in df_day.iterrows():
            if row['sub'] == "UU":
                key = (target_day_int, row['pref'], row['cat'])
                daily_counts[key] = daily_counts.get(key, 0) + 1
        
        update_spreadsheet_daily(daily_counts, target_day_int, target_month_str)
        time.sleep(1)

    # (B) 全体 (詳細) - 月ごとに集計して更新
    df_res['month'] = pd.to_datetime(df_res['date']).dt.month
    for m in sorted(df_res['month'].unique()):
        df_m = df_res[df_res['month'] == m]
        
        total_counts = {}
        for _, row in df_m.iterrows():
            c, s = row['cat'], row['sub']
            label = "不明"
            
            if c == "給湯器":
                if s == "UU": label = "給湯器（UU）"
                elif s == "修理": label = "給湯器（修理）"
                elif s == "止": label = "給湯器（止）"
                else: label = "給湯器（その他電話）"
            elif c == "エコキュート":
                if s == "UU": label = "エコ　（UU）"
                elif s == "修理": label = "エコ　（修理）"
                elif s == "止": label = "エコ　（止）"
                else: label = "エコキュート（その他電話）"
            elif c == "コンロ" and s == "UU": label = "コンロ（UU）"
            elif c == "その他商品" and s == "UU": label = "その他商品（UU）"
            else:
                if c != "その他": label = "不明"
                
            if label != "不明" or (c != "その他"):
                total_counts[label] = total_counts.get(label, 0) + 1
        
        update_spreadsheet_total(total_counts, int(m), "全体")
    
    write_log("完了")

if __name__ == "__main__":
    main()