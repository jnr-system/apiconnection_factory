import requests
import csv
import io
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
from datetime import datetime, timedelta
import calendar
import time
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

RAKURAKU_DOMAIN = "hntobias.rakurakuhanbai.jp"
RAKURAKU_TOKEN = os.environ["RAKURAKU_TOKEN"]

# 2. Googleスプレッドシートの設定
SPREADSHEET_KEY = "1MeMNN29TGNkhpVw6JeSbvGjrSsbx7FYNBU0oo1dkfSE"
LOG_FILE_NAME = "execution_csv_log.txt"

# ★書式設定の定義
THOUSAND_FORMAT = {"numberFormat": {"type": "NUMBER", "pattern": "#,##0,"}}
NORMAL_FORMAT   = {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}

# --------------------------------------------------------------------------
# ★ DB別の集計・書き込み設定リスト
# --------------------------------------------------------------------------
DB_CONFIGS = [
    # ① 既存：正直屋 (DB ID: 101185)
    {
        "name": "正直屋",
        "details_id": "101185",
        "targets": [
            {"label": "先月分", "view_id": "106513", "list_id": "101360"},
            {"label": "今月分", "view_id": "107271", "list_id": "101360"},
            {"label": "来月分", "view_id": "106529", "list_id": "101360"}
        ],
        "keywords": {
            "id": "手配番号", "billing": "請求金額", "date": "施工日",
            "cost": "原価", "construction": "施工金額", "type": "商品タイプ",
            "contract_date": "成約日",
            "status": "進捗",
            "request_type": "依頼内容"
        },
        "contract_targets": [#成約件数と成約請求金額のカウント用
            {"label": "成約_先月", "view_id": "107257", "list_id": "101360"},
            {"label": "成約_今月", "view_id": "107261", "list_id": "101360"},
            {"label": "成約_来月", "view_id": "107265", "list_id": "101360"}
        ],
        "total_settings": {
            "enabled": True,
            "cells": {"billing": "AK17", "cost": "AK18", "const": "AK19", "count": "AK16"},
            "future_cells": {"billing": "AK13", "cost": "AK21", "const": "AK22", "count": "AK12"},
            "contract_cells": {"billing": "AK15", "count": "AK14"}
        },
        "category_settings": [
            {"label": "給湯器", "keyword": "給湯器", "cells": {"billing": "AK35", "cost": "AK36", "const": "AK37", "count": "AK46"}, "future_cells": {"billing": "AK35", "cost": "AK36", "const": "AK37", "count": "AK46"}},
            {"label": "エコキュート", "keyword": "エコキュート", "cells": {"billing": "AK47", "cost": "AK48", "const": "AK49", "count": "AK58"}, "future_cells": {"billing": "AK47", "cost": "AK48", "const": "AK49", "count": "AK58"}},
            {"label": "コンロ", "keyword": "コンロ", "cells": {"billing": "AK59", "cost": "AK60", "const": "AK61", "count": "AK70"}, "future_cells": {"billing": "AK59", "cost": "AK60", "const": "AK61", "count": "AK70"}}
        ]
    },
    # ② 新規：楽天(L-stage) (DB ID: 101357)
    {
        "name": "楽天(L-stage)",
        "details_id": "101357",
        "targets": [
            {"label": "先月分", "view_id": "106541", "list_id": "101365"},
            {"label": "今月分", "view_id": "106545", "list_id": "101365"},
            {"label": "来月分", "view_id": "106549", "list_id": "101365"}
        ],
        "keywords": {
            "id": "提案発注番号", "billing": "売上", "date": "メーカー出荷日",
            "cost": "原価合計", "construction": "粗利", "type": "" 
        },
        "total_settings": {
            "enabled": True,
            "cells": {"billing": "AK104", "cost": "AK105", "const": "AK106"},
            "future_cells": {}
        },
        "category_settings": []
    },
    # ③ 新規：日本設備(JERCY) (DB ID: 101378)
    {
        "name": "日本設備(JERCY)",
        "details_id": "101378",
        "targets": [
            {"label": "先月分", "view_id": "106553", "list_id": "101366"},
            {"label": "今月分", "view_id": "106556", "list_id": "101366"},
            {"label": "来月分", "view_id": "106559", "list_id": "101366"}
        ],
        "keywords": {
            "id": "提案発注番号", "billing": "売上", "date": "メーカー出荷日",
            "cost": "原価合計", "construction": "粗利", "type": "" 
        },
        "total_settings": {
            "enabled": True,
            "cells": {"billing": "AK108", "cost": "AK109", "const": "AK110"},
            "future_cells": {}
        },
        "category_settings": []
    }
]

# ==============================================================================
# 共通関数
# ==============================================================================
LOG_FILE_PATH = Path(__file__).parent / LOG_FILE_NAME

def write_log(message):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{now_str}] {message}"
    print(log_msg)
    try:
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")
    except:
        pass

def to_int(val):
    if not val: return 0
    s = str(val).strip().replace(",", "").replace('"', '').replace("'", "")
    if not s: return 0
    try:
        return int(float(s))
    except:
        return 0

def a1_to_rc(a1_str):
    col_str = ""
    row_str = ""
    for char in a1_str:
        if char.isalpha():
            col_str += char
        else:
            row_str += char
    col_num = 0
    for char in col_str.upper():
        col_num = col_num * 26 + (ord(char) - ord('A') + 1)
    return int(row_str), col_num

def rc_to_a1(row, col):
    div = col
    col_str = ""
    while div > 0:
        div, mod = divmod(div - 1, 26)
        col_str = chr(mod + 65) + col_str
    return f"{col_str}{row}"

# ==============================================================================
# メイン処理
# ==============================================================================
def main():
    write_log("=== 自動集計処理（429エラー対策版）を開始します ===")

    today = datetime.now()
    today_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
    
    range_start = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    range_end   = (today + timedelta(days=31)).replace(hour=23, minute=59, second=59, microsecond=999999)
    
    write_log(f"更新対象期間: {range_start.strftime('%Y/%m/%d')} ～ {range_end.strftime('%Y/%m/%d')}")

    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        workbook = client.open_by_key(SPREADSHEET_KEY)
    except Exception as e:
        write_log(f"スプレッドシート接続エラー: {e}")
        return

    for db_config in DB_CONFIGS:
        write_log(f"■■■ 【{db_config['name']}】(ID:{db_config['details_id']}) の処理を開始 ■■■")
        
        daily_store = {}
        contract_store = {}  # 成約日ベースの集計
        processed_daily_ids = set() # 施工日ベースの重複カウント防止用
        processed_contract_ids = set() # 重複カウント防止のため、処理済みの成約IDを保持
        keywords = db_config["keywords"]

        # データ取得
        for target in db_config["targets"]:
            label = target["label"]
            view_id = target["view_id"]
            list_id = target["list_id"]
            
            write_log(f"--- {label} データ取得中 (View:{view_id}) ---")

            url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
            headers = {"Content-Type": "application/json; charset=utf-8", "X-HD-apitoken": RAKURAKU_TOKEN}
            payload = {
                "dbSchemaId": db_config["details_id"],
                "listId":     list_id,
                "searchId":   view_id,
                "limit":      5000 
            }

            try:
                res = requests.post(url, headers=headers, json=payload, timeout=60)
                if res.status_code != 200:
                    write_log(f"  APIエラー: {res.status_code}")
                    continue 

                try:
                    csv_content = res.content.decode("cp932")
                except:
                    csv_content = res.content.decode("utf-8", errors="ignore")

                f_obj = io.StringIO(csv_content)
                reader = csv.reader(f_obj)
                rows = list(reader)

                if not rows:
                    write_log("  データなし")
                    continue

                header_row = rows[0]
                def find_idx(keyword):
                    if not keyword: return -1
                    for i, col_name in enumerate(header_row):
                        if keyword in col_name: return i
                    return -1

                idx_id    = find_idx(keywords["id"])
                idx_bill  = find_idx(keywords["billing"])
                idx_date  = find_idx(keywords["date"])
                idx_cost  = find_idx(keywords["cost"])
                idx_const = find_idx(keywords["construction"])
                idx_type  = find_idx(keywords["type"])

                if idx_date == -1:
                    write_log(f"  エラー: 日付列『{keywords['date']}』が見つかりません。")
                    continue

                max_idx = max(idx_id, idx_bill, idx_date, idx_cost, idx_const, idx_type)
                count_rows = 0

                for row in rows[1:]:
                    if len(row) <= max_idx: continue

                    # IDを取得して重複チェック
                    val_id = ""
                    if idx_id != -1:
                        val_id = row[idx_id].strip().replace('"', '').replace("'", "")
                    
                    # IDがないか、既に処理済みの場合はスキップ
                    if not val_id or val_id in processed_daily_ids:
                        continue

                    val_bill  = to_int(row[idx_bill])  if idx_bill  != -1 else 0
                    val_cost  = to_int(row[idx_cost])  if idx_cost  != -1 else 0
                    val_const = to_int(row[idx_const]) if idx_const != -1 else 0
                    val_type  = row[idx_type].strip()  if idx_type  != -1 else ""

                    # ── 施工日ベースの集計 ──
                    val_date_str = row[idx_date].strip().replace('"', '').replace("'", "")
                    if not val_date_str: continue
                    try: dt = datetime.strptime(val_date_str, "%Y/%m/%d")
                    except: continue

                    if not (range_start <= dt <= range_end): continue
                    fmt_date = dt.strftime("%Y/%m/%d")

                    if fmt_date not in daily_store: daily_store[fmt_date] = {}

                    if "total" not in daily_store[fmt_date]:
                        daily_store[fmt_date]["total"] = {"billing":0, "cost":0, "const":0, "count":0}

                    daily_store[fmt_date]["total"]["billing"] += val_bill
                    daily_store[fmt_date]["total"]["cost"]    += val_cost
                    daily_store[fmt_date]["total"]["const"]   += val_const
                    if val_bill > 0: daily_store[fmt_date]["total"]["count"] += 1

                    if db_config["category_settings"]:
                        matched_label = None
                        if val_type:
                            for cat in db_config["category_settings"]:
                                if cat["keyword"] in val_type:
                                    matched_label = cat["label"]
                                    break
                        if matched_label:
                            if matched_label not in daily_store[fmt_date]:
                                daily_store[fmt_date][matched_label] = {"billing":0, "cost":0, "const":0, "count":0}
                            daily_store[fmt_date][matched_label]["billing"] += val_bill
                            daily_store[fmt_date][matched_label]["cost"]    += val_cost
                            daily_store[fmt_date][matched_label]["const"]   += val_const
                            if val_bill > 0: daily_store[fmt_date][matched_label]["count"] += 1

                    processed_daily_ids.add(val_id) # 処理済みIDとしてセットに追加
                    count_rows += 1

                write_log(f"  集計対象: {count_rows} 件")
                time.sleep(2) # 取得後も少し待つ

            except Exception as e:
                write_log(f"  処理エラー: {e}")

        # 成約日専用データ取得（複数ビュー対応）
        c_targets = db_config.get("contract_targets", [])
        for c_target in c_targets:
            c_label   = c_target["label"]
            c_view_id = c_target["view_id"]
            c_list_id = c_target["list_id"]
            
            if not c_view_id: continue

            write_log(f"--- 成約日データ取得中: {c_label} (View:{c_view_id}) ---")
            try:
                url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
                headers = {"Content-Type": "application/json; charset=utf-8", "X-HD-apitoken": RAKURAKU_TOKEN}
                payload = {
                    "dbSchemaId": db_config["details_id"],
                    "listId":     c_list_id,
                    "searchId":   c_view_id,
                    "limit":      5000
                }
                res = requests.post(url, headers=headers, json=payload, timeout=60)
                if res.status_code != 200:
                    write_log(f"  APIエラー: {res.status_code}")
                    continue

                try:
                    try:
                        csv_content = res.content.decode("cp932")
                    except:
                        csv_content = res.content.decode("utf-8", errors="ignore")

                    f_obj = io.StringIO(csv_content)
                    rows_c = list(csv.reader(f_obj))

                    if not rows_c:
                        write_log(f"  データなし ({c_label})")
                    else:
                        header_row_c = rows_c[0]
                        def find_idx_c(keyword):
                            if not keyword: return -1
                            for i, col_name in enumerate(header_row_c):
                                if keyword in col_name: return i
                            return -1

                        c_idx_bill = find_idx_c(keywords["billing"])
                        c_idx_date = find_idx_c(keywords.get("contract_date", ""))
                        c_idx_status = find_idx_c(keywords.get("status", ""))
                        c_idx_id = find_idx_c(keywords["id"])
                        c_idx_req = find_idx_c(keywords.get("request_type", ""))

                        if c_idx_date == -1:
                            write_log(f"  エラー: 成約日列『{keywords.get('contract_date', '')}』が見つかりません。")
                        else:
                            c_max_idx = max(
                                c_idx_bill if c_idx_bill != -1 else 0,
                                c_idx_date,
                                c_idx_status if c_idx_status != -1 else 0,
                                c_idx_id if c_idx_id != -1 else 0,
                                c_idx_req if c_idx_req != -1 else 0
                            )
                            count_rows_c = 0
                            for row in rows_c[1:]:
                                if len(row) <= c_max_idx: continue

                                # 1. IDを取得し、重複をチェック
                                val_id = ""
                                if c_idx_id != -1:
                                    val_id = row[c_idx_id].strip().replace('"', '').replace("'", "")

                                if not val_id or val_id in processed_contract_ids:
                                    continue

                                # 2. 元のフィルタリングロジックを維持
                                if "JNR-" not in val_id:
                                    continue

                                if c_idx_status != -1:
                                    val_status = row[c_idx_status].strip().replace('"', '').replace("'", "")
                                    if val_status == "重複":
                                        continue

                                if c_idx_req != -1:
                                    val_req = row[c_idx_req].strip().replace('"', '').replace("'", "")
                                    if val_req != "施工" and val_req != "" and val_req != "施工不可":
                                        continue

                                val_cdate_str = row[c_idx_date].strip().replace('"', '').replace("'", "")
                                if not val_cdate_str: continue
                                try: dt_c = datetime.strptime(val_cdate_str, "%Y/%m/%d")
                                except: continue
                                if not (range_start <= dt_c <= range_end): continue

                                fmt_cdate = dt_c.strftime("%Y/%m/%d")
                                val_bill  = to_int(row[c_idx_bill]) if c_idx_bill != -1 else 0
                                if fmt_cdate not in contract_store:
                                    contract_store[fmt_cdate] = {"billing": 0, "count": 0}
                                contract_store[fmt_cdate]["billing"] += val_bill
                                # 成約数は条件を満たせばカウント (金額判定なし)
                                contract_store[fmt_cdate]["count"] += 1

                                processed_contract_ids.add(val_id) # 処理済みIDとしてセットに追加
                                count_rows_c += 1
                            write_log(f"  成約日集計対象({c_label}): {count_rows_c} 件")
                except Exception as parse_err:
                    write_log(f"  CSV解析エラー: {parse_err}")
                time.sleep(2)
            except Exception as e:
                write_log(f"  処理エラー: {e}")

        # 書き込み処理
        target_months = set()
        curr = range_start
        while curr <= range_end:
            ym = curr.strftime("%Y%m")
            target_months.add(ym)
            next_month = (curr.replace(day=1) + timedelta(days=32)).replace(day=1)
            curr = next_month

        for ym in sorted(target_months):
            sheet_name = f"{ym}"
            year = int(ym[:4])
            month = int(ym[4:])
            last_day = calendar.monthrange(year, month)[1]
            month_start_dt = datetime(year, month, 1)
            month_end_dt   = datetime(year, month, last_day, 23, 59, 59)
            update_start_dt = max(month_start_dt, range_start)
            update_end_dt   = min(month_end_dt, range_end)

            if update_start_dt > update_end_dt: continue

            start_day = update_start_dt.day
            end_day   = update_end_dt.day

            write_log(f"--- シート [{sheet_name}] 書き込み ({month}/{start_day} ～ {month}/{end_day}) ---")

            try:
                worksheet = workbook.worksheet(sheet_name)
            except:
                write_log(f"  スキップ: シート {sheet_name} なし")
                continue

            MIN_COL_COUNT = 80
            if worksheet.col_count < MIN_COL_COUNT:
                try: worksheet.resize(cols=MIN_COL_COUNT); time.sleep(1)
                except: pass

            def write_separate_data(label_key, settings_dict):
                if not settings_dict: return
                
                past_cells   = settings_dict.get("cells", {})
                future_cells = settings_dict.get("future_cells", {})
                p_bill, p_cost, p_const, p_count = [], [], [], []
                f_bill, f_cost, f_const, f_count = [], [], [], []

                for day in range(start_day, end_day + 1):
                    date_key = f"{year}/{month:02}/{day:02}"
                    data = None
                    if date_key in daily_store:
                        if label_key == "total": data = daily_store[date_key].get("total")
                        else: data = daily_store[date_key].get(label_key)
                    
                    val_bill  = data["billing"] if data else 0
                    val_cost  = data["cost"]    if data else 0
                    val_const = data["const"]   if data else 0
                    val_count = data["count"]   if data else 0

                    if datetime(year, month, day) < today_date:
                        p_bill.append(val_bill); p_cost.append(val_cost); p_const.append(val_const); p_count.append(val_count)
                        f_bill.append(""); f_cost.append(""); f_const.append(""); f_count.append("")
                    else:
                        p_bill.append(""); p_cost.append(""); p_const.append(""); p_count.append("")
                        if label_key == "total":
                            f_bill.append(val_bill); f_cost.append(""); f_const.append(""); f_count.append(val_count)
                        else:
                            f_bill.append(""); f_cost.append(""); f_const.append(""); f_count.append("")

                def smart_update(p_cell, f_cell, p_data, f_data, is_count=False):
                    if not p_cell and not f_cell: return
                    
                    # リトライ付き書き込み関数
                    def update_and_format_with_retry(cell_addr, vals):
                        max_retries = 5
                        for i in range(max_retries):
                            try:
                                r, c = a1_to_rc(cell_addr)
                                worksheet.update(range_name=rc_to_a1(r, c + (start_day - 1)), values=[vals])
                                
                                if is_count:
                                    end_col = c + (start_day - 1) + len(vals) - 1
                                    worksheet.format(f"{rc_to_a1(r, c + (start_day - 1))}:{rc_to_a1(r, end_col)}", NORMAL_FORMAT)
                                else:
                                    end_col = c + (start_day - 1) + len(vals) - 1
                                    worksheet.format(f"{rc_to_a1(r, c + (start_day - 1))}:{rc_to_a1(r, end_col)}", THOUSAND_FORMAT)
                                
                                time.sleep(2) # 成功しても2秒待つ（重要）
                                return # 成功したら抜ける
                            except Exception as e:
                                if "429" in str(e) or "Quota exceeded" in str(e):
                                    wait_time = (2 ** i) + 2 # 3, 4, 6, 10, 18秒待機
                                    write_log(f"    レート制限(429)検知。{wait_time}秒待機して再試行します... ({i+1}/{max_retries})")
                                    time.sleep(wait_time)
                                else:
                                    write_log(f"    書き込みエラー: {e}")
                                    break # 429以外は諦める

                    if p_cell and f_cell and p_cell == f_cell:
                        combined = [p_data[i] if p_data[i] != "" else f_data[i] for i in range(len(p_data))]
                        update_and_format_with_retry(p_cell, combined)
                    else:
                        if p_cell: update_and_format_with_retry(p_cell, p_data)
                        if f_cell: update_and_format_with_retry(f_cell, f_data)

                smart_update(past_cells.get("billing"), future_cells.get("billing"), p_bill, f_bill, is_count=False)
                smart_update(past_cells.get("cost"),    future_cells.get("cost"),    p_cost, f_cost, is_count=False)
                smart_update(past_cells.get("const"),   future_cells.get("const"),   p_const, f_const, is_count=False)
                smart_update(past_cells.get("count"),   future_cells.get("count"),   p_count, f_count, is_count=True)

            if db_config["total_settings"]["enabled"]:
                write_separate_data("total", db_config["total_settings"])
            for cat in db_config["category_settings"]:
                write_separate_data(cat["label"], cat)

            # 成約日ベースの書き込み
            def write_contract_data(contract_cells):
                if not contract_cells: return
                bill_cell  = contract_cells.get("billing")
                count_cell = contract_cells.get("count")
                p_bill, p_count = [], []
                for day in range(start_day, end_day + 1):
                    date_key = f"{year}/{month:02}/{day:02}"
                    if datetime(year, month, day) < today_date:
                        data = contract_store.get(date_key)
                        p_bill.append(data["billing"] if data else 0)
                        p_count.append(data["count"]  if data else 0)
                    else:
                        p_bill.append("")
                        p_count.append("")

                def _update_contract(cell_addr, vals, is_count=False):
                    max_retries = 5
                    for i in range(max_retries):
                        try:
                            r, c = a1_to_rc(cell_addr)
                            worksheet.update(range_name=rc_to_a1(r, c + (start_day - 1)), values=[vals])
                            end_col = c + (start_day - 1) + len(vals) - 1
                            fmt = NORMAL_FORMAT if is_count else THOUSAND_FORMAT
                            worksheet.format(f"{rc_to_a1(r, c + (start_day - 1))}:{rc_to_a1(r, end_col)}", fmt)
                            time.sleep(2)
                            return
                        except Exception as e:
                            if "429" in str(e) or "Quota exceeded" in str(e):
                                wait_time = (2 ** i) + 2
                                write_log(f"    レート制限(429)検知。{wait_time}秒待機して再試行します... ({i+1}/{max_retries})")
                                time.sleep(wait_time)
                            else:
                                write_log(f"    書き込みエラー: {e}")
                                break

                if bill_cell:  _update_contract(bill_cell,  p_bill,  is_count=False)
                if count_cell: _update_contract(count_cell, p_count, is_count=True)

            contract_cells = db_config["total_settings"].get("contract_cells", {})
            if contract_cells:
                write_contract_data(contract_cells)

    write_log("=== 全処理完了 ===")

if __name__ == "__main__":
    main()