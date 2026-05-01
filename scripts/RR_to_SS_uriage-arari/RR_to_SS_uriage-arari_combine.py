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
            {"label": "今年分（統合）", "view_id": "106513", "list_id": "101360"}
        ],
        "keywords": {
            "id": "手配番号", "billing": "請求金額", "date": "施工日",
            "cost": "原価", "construction": "施工金額", "type": "商品タイプ",
            "contract_date": "成約日",
            "status": "進捗",
            "request_type": "依頼内容",
            "profit": "利益"
        },
        "contract_targets": [
            {"label": "成約_今年分（統合）", "view_id": "107257", "list_id": "101360"}
        ],
        "total_settings": {
            "enabled": True,
            "cells": {"billing": "", "cost": "", "const": "", "count": "AK25"},
            "future_cells": {"billing": "AK22", "cost": "", "const": "", "count": "", "count_b": "AK24", "profit": "AK23"},
            "extra_future_cells": {"billing": "AK29", "cost": "AK30", "const": "AK31"},
            "contract_cells": {
                "billing": "AK13", "count": "AK12",
                "cancel_count": "AK14",
                "sum_count": "AK15",
                "complete_count": "AK16",
                "next_month_count": "AK17",
                "rework_current_month_count": "AK18", "rework_current_month_count_b": "AK26",
                "rework_next_month_count": "AK19", "rework_next_month_count_b": "AK27",
                "pending_count": "AK20"
            }
        },
        "category_settings": [
            {"label": "給湯器", "keyword": "給湯器",
             "cells": {"billing": "AK47", "cost": "AK48", "const": "AK49", "count": ""},
             "future_cells": {"billing": "AK69", "cost": "", "const": "", "count": "AK70"},
             "extra_count_cell": "AK58", "extra_count_cell_b": "AK71",
             "contract_cells": {
                 "billing": "AK60", "count": "AK59",
                 "cancel_count": "AK61",
                 "sum_count": "AK62",
                 "complete_count": "AK63",
                 "next_month_count": "AK64",
                 "rework_current_month_count": "AK65", "rework_current_month_count_b": "AK72",
                 "rework_next_month_count": "AK66", "rework_next_month_count_b": "AK73",
                 "pending_count": "AK67"
             }},
            {"label": "エコキュート", "keyword": "エコキュート",
             "cells": {"billing": "AK74", "cost": "AK75", "const": "AK76", "count": ""},
             "future_cells": {"billing": "AK96", "cost": "", "const": "", "count": "AK97"},
             "extra_count_cell": "AK85", "extra_count_cell_b": "AK98",
             "contract_cells": {
                 "billing": "AK87", "count": "AK86",
                 "cancel_count": "AK88",
                 "sum_count": "AK89",
                 "complete_count": "AK90",
                 "next_month_count": "AK91",
                 "rework_current_month_count": "AK92", "rework_current_month_count_b": "AK99",
                 "rework_next_month_count": "AK93", "rework_next_month_count_b": "AK100",
                 "pending_count": "AK94"
             }},
            {"label": "コンロ", "keyword": "コンロ",
             "cells": {"billing": "AK101", "cost": "AK102", "const": "AK103", "count": ""},
             "future_cells": {"billing": "AK123", "cost": "", "const": "", "count": "AK124"},
             "extra_count_cell": "AK112", "extra_count_cell_b": "AK125",
             "contract_cells": {
                 "billing": "AK114", "count": "AK113",
                 "cancel_count": "AK115",
                 "sum_count": "AK116",
                 "complete_count": "AK117",
                 "next_month_count": "AK118",
                 "rework_current_month_count": "AK119", "rework_current_month_count_b": "AK126",
                 "rework_next_month_count": "AK120", "rework_next_month_count_b": "AK127",
                 "pending_count": "AK121"
             }}
        ]
    },
    # ② 新規：楽天(L-stage) (DB ID: 101357)
    {
        "name": "楽天(L-stage)",
        "details_id": "101357",
        "targets": [
            {"label": "今年分（統合）", "view_id": "106541", "list_id": "101365"}
        ],
        "keywords": {
            "id": "提案発注番号", "billing": "売上", "date": "メーカー出荷日",
            "cost": "原価合計", "construction": "粗利", "type": "" 
        },
        "total_settings": {
            "enabled": True,
            "cells": {"billing": "AK161", "cost": "AK162", "const": "AK163"},
            "future_cells": {}
        },
        "category_settings": []
    },
    # ③ 新規：日本設備(JERCY) (DB ID: 101378)
    {
        "name": "日本設備(JERCY)",
        "details_id": "101378",
        "targets": [
            {"label": "今年分（統合）", "view_id": "106553", "list_id": "101366"}
        ],
        "keywords": {
            "id": "提案発注番号", "billing": "売上", "date": "メーカー出荷日",
            "cost": "原価合計", "construction": "粗利", "type": "" 
        },
        "total_settings": {
            "enabled": True,
            "cells": {"billing": "AK165", "cost": "AK166", "const": "AK167"},
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
    
    range_start = (today - timedelta(days=15)).replace(hour=0, minute=0, second=0, microsecond=0)
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
        extra_contract_store = {} # 正直屋専用：当月完了/次月繰越/再工事 などを成約日キーで集計
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

                idx_id     = find_idx(keywords["id"])
                idx_bill   = find_idx(keywords["billing"])
                idx_date   = find_idx(keywords["date"])
                idx_cost   = find_idx(keywords["cost"])
                idx_const  = find_idx(keywords["construction"])
                idx_type   = find_idx(keywords["type"])
                idx_profit = find_idx(keywords.get("profit", ""))

                if idx_date == -1:
                    write_log(f"  エラー: 日付列『{keywords['date']}』が見つかりません。")
                    continue

                max_idx = max(idx_id, idx_bill, idx_date, idx_cost, idx_const, idx_type, idx_profit if idx_profit != -1 else 0)
                count_rows = 0

                for row in rows[1:]:
                    if len(row) <= max_idx: continue

                    # IDを取得して重複チェック
                    val_id = ""
                    if idx_id != -1:
                        val_id = row[idx_id].strip().replace('"', '').replace("'", "")
                    
                    if not val_id or val_id in processed_daily_ids:
                        continue

                    val_bill   = to_int(row[idx_bill])   if idx_bill   != -1 else 0
                    val_cost   = to_int(row[idx_cost])   if idx_cost   != -1 else 0
                    val_const  = to_int(row[idx_const])  if idx_const  != -1 else 0
                    val_type   = row[idx_type].strip()   if idx_type   != -1 else ""
                    val_profit = to_int(row[idx_profit]) if idx_profit != -1 else 0

                    # ── 施工日ベースの集計 ──
                    val_date_str = row[idx_date].strip().replace('"', '').replace("'", "")
                    if not val_date_str: continue
                    try: dt = datetime.strptime(val_date_str, "%Y/%m/%d")
                    except: continue

                    if not (range_start <= dt <= range_end): continue
                    fmt_date = dt.strftime("%Y/%m/%d")

                    if fmt_date not in daily_store: daily_store[fmt_date] = {}

                    if "total" not in daily_store[fmt_date]:
                        daily_store[fmt_date]["total"] = {"billing":0, "cost":0, "const":0, "count":0, "profit":0}

                    daily_store[fmt_date]["total"]["billing"] += val_bill
                    daily_store[fmt_date]["total"]["cost"]    += val_cost
                    daily_store[fmt_date]["total"]["const"]   += val_const
                    daily_store[fmt_date]["total"]["profit"]  += val_profit
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
                                daily_store[fmt_date][matched_label] = {"billing":0, "cost":0, "const":0, "count":0, "profit":0}
                            daily_store[fmt_date][matched_label]["billing"] += val_bill
                            daily_store[fmt_date][matched_label]["cost"]    += val_cost
                            daily_store[fmt_date][matched_label]["const"]   += val_const
                            daily_store[fmt_date][matched_label]["profit"]  += val_profit
                            if val_bill > 0: daily_store[fmt_date][matched_label]["count"] += 1

                    processed_daily_ids.add(val_id)
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
                        c_idx_sdate = find_idx_c(keywords.get("date", "")) # 施工日
                        c_idx_type = find_idx_c(keywords.get("type", "")) # 商品タイプ

                        if c_idx_date == -1:
                            write_log(f"  エラー: 成約日列『{keywords.get('contract_date', '')}』が見つかりません。")
                        else:
                            c_max_idx = max(
                                c_idx_bill if c_idx_bill != -1 else 0,
                                c_idx_date,
                                c_idx_status if c_idx_status != -1 else 0,
                                c_idx_id if c_idx_id != -1 else 0,
                                c_idx_req if c_idx_req != -1 else 0,
                                c_idx_sdate if c_idx_sdate != -1 else 0,
                                c_idx_type if c_idx_type != -1 else 0
                            )
                            count_rows_c = 0
                            for row in rows_c[1:]:
                                if len(row) <= c_max_idx: continue

                                # 1. ID取得と重複チェック
                                val_id = ""
                                if c_idx_id != -1:
                                    val_id = row[c_idx_id].strip().replace('"', '').replace("'", "")
                                
                                if not val_id or val_id in processed_contract_ids:
                                    continue

                                if "JNR-" not in val_id:
                                    continue

                                # 2. 進捗チェック (重複以外)
                                val_status = row[c_idx_status].strip().replace('"', '').replace("'", "") if c_idx_status != -1 else ""
                                if val_status == "重複":
                                    continue

                                val_req = row[c_idx_req].strip().replace('"', '').replace("'", "") if c_idx_req != -1 else ""

                                # ========= 正直屋 と 他DB(楽天等)の分岐 =========
                                is_shoujikiya = (db_config["name"] == "正直屋")

                                if not is_shoujikiya:
                                    # ----------------------------------------------------
                                    # [正直屋以外(楽天・日本設備等)のロジック] 従来通り
                                    # ----------------------------------------------------
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
                                        contract_store[fmt_cdate] = {"total": {"billing": 0, "count": 0}}
                                    
                                    # [1] total
                                    contract_store[fmt_cdate]["total"]["billing"] += val_bill
                                    contract_store[fmt_cdate]["total"]["count"] += 1

                                    count_rows_c += 1

                                else:
                                    # ----------------------------------------------------
                                    # [正直屋のロジック] shoujikiya_uriage_api.py ベース
                                    # ----------------------------------------------------
                                    val_cdate_str = row[c_idx_date].strip().replace('"', '').replace("'", "")
                                    if not val_cdate_str: continue
                                    try: dt_c = datetime.strptime(val_cdate_str, "%Y/%m/%d")
                                    except: continue
                                    if not (range_start <= dt_c <= range_end): continue

                                    fmt_cdate = dt_c.strftime("%Y/%m/%d")
                                    val_bill  = to_int(row[c_idx_bill]) if c_idx_bill != -1 else 0
                                    val_type_c = row[c_idx_type].strip().replace('"', '').replace("'", "") if c_idx_type != -1 else ""

                                    is_cancel_c = "キャンセル" in val_status

                                    # カテゴリ判定
                                    matched_cat = None
                                    if val_type_c:
                                        for cat in db_config["category_settings"]:
                                            if cat["keyword"] in val_type_c:
                                                matched_cat = cat["label"]
                                                break

                                    # ヘルパー関数群
                                    def _ensure_extra(key):
                                        if fmt_cdate not in extra_contract_store:
                                            extra_contract_store[fmt_cdate] = {}
                                        if key not in extra_contract_store[fmt_cdate]:
                                            extra_contract_store[fmt_cdate][key] = {
                                                "cancel_count": 0, "complete_count": 0,
                                                "next_month_count": 0,
                                                "rework_current_month_count": 0,
                                                "rework_next_month_count": 0,
                                                "pending_count": 0
                                            }

                                    def _ensure_contract(key):
                                        if fmt_cdate not in contract_store:
                                            contract_store[fmt_cdate] = {}
                                        if key not in contract_store[fmt_cdate]:
                                            contract_store[fmt_cdate][key] = {"billing": 0, "count": 0}

                                    # ① キャンセル (最優先・排他)
                                    if is_cancel_c:
                                        _ensure_extra("total")
                                        extra_contract_store[fmt_cdate]["total"]["cancel_count"] += 1
                                        _ensure_contract("total")
                                        contract_store[fmt_cdate]["total"]["billing"] += val_bill
                                        contract_store[fmt_cdate]["total"]["count"] += 1
                                        if matched_cat:
                                            _ensure_extra(matched_cat)
                                            extra_contract_store[fmt_cdate][matched_cat]["cancel_count"] += 1
                                            _ensure_contract(matched_cat)
                                            contract_store[fmt_cdate][matched_cat]["billing"] += val_bill
                                            contract_store[fmt_cdate][matched_cat]["count"] += 1

                                    # ② 再工事 (排他)
                                    elif val_req == "再工事":
                                        _ensure_extra("total")
                                        val_sdate_rw = row[c_idx_sdate].strip().replace('"', '').replace("'", "") if c_idx_sdate != -1 else ""
                                        is_rework_current = True
                                        if val_sdate_rw:
                                            try:
                                                dt_s_rw = datetime.strptime(val_sdate_rw, "%Y/%m/%d")
                                                if (dt_s_rw.year * 12 + dt_s_rw.month) > (dt_c.year * 12 + dt_c.month):
                                                    is_rework_current = False
                                            except:
                                                pass
                                        
                                        if is_rework_current:
                                            extra_contract_store[fmt_cdate]["total"]["rework_current_month_count"] += 1
                                        else:
                                            extra_contract_store[fmt_cdate]["total"]["rework_next_month_count"] += 1

                                        _ensure_contract("total")
                                        contract_store[fmt_cdate]["total"]["count"] += 1

                                        if matched_cat:
                                            _ensure_extra(matched_cat)
                                            if is_rework_current:
                                                extra_contract_store[fmt_cdate][matched_cat]["rework_current_month_count"] += 1
                                            else:
                                                extra_contract_store[fmt_cdate][matched_cat]["rework_next_month_count"] += 1
                                            _ensure_contract(matched_cat)
                                            contract_store[fmt_cdate][matched_cat]["count"] += 1

                                    # ③ 有効な成約 (施工/空白/施工不可。金額0円も含む（RR_to_SS側仕様優先）)
                                    elif val_req in ("施工", "", "施工不可"):
                                        _ensure_contract("total")
                                        # (★0円でも件数はカウント、金額は加算)
                                        contract_store[fmt_cdate]["total"]["billing"] += val_bill
                                        if matched_cat:
                                            _ensure_contract(matched_cat)
                                            contract_store[fmt_cdate][matched_cat]["billing"] += val_bill
                                        
                                        count_rows_c += 1

                                        is_complete_c = False
                                        is_next_month_c = False
                                        is_pending_c = False

                                        val_sdate_str = row[c_idx_sdate].strip().replace('"', '').replace("'", "") if c_idx_sdate != -1 else ""
                                        if not val_sdate_str:
                                            is_pending_c = True
                                        else:
                                            try:
                                                dt_s = datetime.strptime(val_sdate_str, "%Y/%m/%d")
                                                if dt_c.year == dt_s.year and dt_c.month == dt_s.month:
                                                    is_complete_c = True
                                                elif (dt_s.year * 12 + dt_s.month) > (dt_c.year * 12 + dt_c.month):
                                                    is_next_month_c = True
                                            except:
                                                is_pending_c = True

                                        _ensure_extra("total")
                                        if is_complete_c:
                                            extra_contract_store[fmt_cdate]["total"]["complete_count"] += 1
                                            contract_store[fmt_cdate]["total"]["count"] += 1
                                        elif is_next_month_c:
                                            extra_contract_store[fmt_cdate]["total"]["next_month_count"] += 1
                                            contract_store[fmt_cdate]["total"]["count"] += 1
                                        elif is_pending_c:
                                            extra_contract_store[fmt_cdate]["total"]["pending_count"] += 1
                                            contract_store[fmt_cdate]["total"]["count"] += 1

                                        if matched_cat:
                                            _ensure_extra(matched_cat)
                                            if is_complete_c:
                                                extra_contract_store[fmt_cdate][matched_cat]["complete_count"] += 1
                                                contract_store[fmt_cdate][matched_cat]["count"] += 1
                                            elif is_next_month_c:
                                                extra_contract_store[fmt_cdate][matched_cat]["next_month_count"] += 1
                                                contract_store[fmt_cdate][matched_cat]["count"] += 1
                                            elif is_pending_c:
                                                extra_contract_store[fmt_cdate][matched_cat]["pending_count"] += 1
                                                contract_store[fmt_cdate][matched_cat]["count"] += 1
                                
                                processed_contract_ids.add(val_id)
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
                p_bill, p_cost, p_const, p_count, p_profit = [], [], [], [], []
                f_bill, f_cost, f_const, f_count, f_profit = [], [], [], [], []

                for day in range(start_day, end_day + 1):
                    date_key = f"{year}/{month:02}/{day:02}"
                    data = None
                    if date_key in daily_store:
                        if label_key == "total": data = daily_store[date_key].get("total")
                        else: data = daily_store[date_key].get(label_key)

                    val_bill   = data["billing"] if data else 0
                    val_cost   = data["cost"]    if data else 0
                    val_const  = data["const"]   if data else 0
                    val_count  = data["count"]   if data else 0
                    val_profit = data["profit"]  if data and "profit" in data else 0

                    if datetime(year, month, day) < today_date:
                        p_bill.append(val_bill); p_cost.append(val_cost); p_const.append(val_const); p_count.append(val_count); p_profit.append(val_profit)
                        f_bill.append(""); f_cost.append(""); f_const.append(""); f_count.append(""); f_profit.append("")
                    else:
                        p_bill.append(""); p_cost.append(""); p_const.append(""); p_count.append(""); p_profit.append("")
                        f_bill.append(val_bill); f_cost.append(val_cost); f_const.append(val_const); f_count.append(val_count); f_profit.append(val_profit)

                def smart_update(p_cell, f_cell, p_data, f_data, is_count=False, preserve_past=False):
                    if not p_cell and not f_cell: return

                    # リトライ付き書き込み関数
                    # preserve_past=True の場合、先頭の "" をスキップして書き込み開始列をずらす
                    # （過去分のセルを上書きしないため）
                    def update_and_format_with_retry(cell_addr, vals, skip_empty_prefix=False):
                        r, c = a1_to_rc(cell_addr)
                        offset = start_day - 1
                        if skip_empty_prefix:
                            first_valid = next((i for i, v in enumerate(vals) if v != ""), None)
                            if first_valid is None:
                                return  # 書き込むデータなし
                            offset += first_valid
                            vals = vals[first_valid:]
                        write_cell = rc_to_a1(r, c + offset)
                        end_col = c + offset + len(vals) - 1
                        max_retries = 5
                        for i in range(max_retries):
                            try:
                                worksheet.update(range_name=write_cell, values=[vals])
                                fmt = NORMAL_FORMAT if is_count else THOUSAND_FORMAT
                                worksheet.format(f"{write_cell}:{rc_to_a1(r, end_col)}", fmt)
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
                        if f_cell: update_and_format_with_retry(f_cell, f_data, skip_empty_prefix=preserve_past)

                smart_update(past_cells.get("billing"), future_cells.get("billing"), p_bill, f_bill, is_count=False)
                smart_update(past_cells.get("cost"),    future_cells.get("cost"),    p_cost, f_cost, is_count=False)
                smart_update(past_cells.get("const"),   future_cells.get("const"),   p_const, f_const, is_count=False)
                smart_update(past_cells.get("profit"),  future_cells.get("profit"),  p_profit, f_profit, is_count=False)
                smart_update(past_cells.get("count"),   future_cells.get("count"),   p_count, f_count, is_count=True, preserve_past=True)
                if future_cells.get("count_b"):
                    smart_update(None, future_cells.get("count_b"), p_count, f_count, is_count=True, preserve_past=True)

                extra_count_cell = settings_dict.get("extra_count_cell")
                if extra_count_cell:
                    smart_update(extra_count_cell, None, p_count, f_count, is_count=True)

                extra_count_cell_b = settings_dict.get("extra_count_cell_b")
                if extra_count_cell_b:
                    smart_update(extra_count_cell_b, None, p_count, f_count, is_count=True)

                extra_future = settings_dict.get("extra_future_cells", {})
                if extra_future:
                    smart_update(extra_future.get("billing"), None, p_bill, f_bill, is_count=False)
                    smart_update(extra_future.get("cost"),    None, p_cost, f_cost, is_count=False)
                    smart_update(extra_future.get("const"),   None, p_const, f_const, is_count=False)

            if db_config["total_settings"]["enabled"]:
                write_separate_data("total", db_config["total_settings"])
            for cat in db_config["category_settings"]:
                write_separate_data(cat["label"], cat)

            # 成約日ベースの書き込み
            def write_contract_data(label_key, contract_cells):
                if not contract_cells: return
                bill_cell  = contract_cells.get("billing")
                count_cell = contract_cells.get("count")
                p_bill, p_count = [], []
                for day in range(start_day, end_day + 1):
                    date_key = f"{year}/{month:02}/{day:02}"
                    if datetime(year, month, day) < today_date:
                        data = contract_store.get(date_key, {}).get(label_key)
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

            write_contract_data("total", db_config["total_settings"].get("contract_cells", {}))
            for cat in db_config["category_settings"]:
                write_contract_data(cat["label"], cat.get("contract_cells", {}))

            # 正直屋のみ：成約日ベース（当月完了/次月繰越/当月再工事/次月再工事）の書き込み
            if db_config["name"] == "正直屋":
                def write_extra_contract_data(label_key, contract_cells):
                    if not contract_cells: return
                    cancel_cell          = contract_cells.get("cancel_count")
                    sum_cell             = contract_cells.get("sum_count")
                    complete_cell        = contract_cells.get("complete_count")
                    complete_cell_b      = contract_cells.get("complete_count_b")
                    next_month_cell      = contract_cells.get("next_month_count")
                    next_month_cell_b    = contract_cells.get("next_month_count_b")
                    rework_cur_cell      = contract_cells.get("rework_current_month_count")
                    rework_cur_cell_b    = contract_cells.get("rework_current_month_count_b")
                    rework_next_cell     = contract_cells.get("rework_next_month_count")
                    rework_next_cell_b   = contract_cells.get("rework_next_month_count_b")
                    pending_cell         = contract_cells.get("pending_count")
                    if not any([cancel_cell, complete_cell, complete_cell_b,
                                next_month_cell, next_month_cell_b,
                                rework_cur_cell, rework_cur_cell_b,
                                rework_next_cell, rework_next_cell_b,
                                pending_cell, sum_cell]):
                        return

                    p_cancel, p_complete, p_next_month = [], [], []
                    p_rework_cur, p_rework_next, p_pending = [], [], []
                    p_sum = []
                    for day in range(start_day, end_day + 1):
                        date_key = f"{year}/{month:02}/{day:02}"
                        if datetime(year, month, day) < today_date:
                            extra = extra_contract_store.get(date_key, {})
                            d = extra.get("total") if label_key == "total" else extra.get(label_key)
                            c_cancel     = d["cancel_count"]                   if d else 0
                            c_complete   = d["complete_count"]                 if d else 0
                            c_next_month = d["next_month_count"]               if d else 0
                            c_rework_cur = d["rework_current_month_count"]     if d else 0
                            c_rework_next= d["rework_next_month_count"]        if d else 0
                            c_pending    = d["pending_count"]                  if d else 0

                            p_cancel.append(c_cancel)
                            p_complete.append(c_complete)
                            p_next_month.append(c_next_month)
                            p_rework_cur.append(c_rework_cur)
                            p_rework_next.append(c_rework_next)
                            p_pending.append(c_pending)
                            p_sum.append(c_complete + c_next_month + c_rework_cur + c_rework_next)
                        else:
                            p_cancel.append(""); p_complete.append(""); p_next_month.append("")
                            p_rework_cur.append(""); p_rework_next.append(""); p_pending.append("")
                            p_sum.append("")

                    def _upd(cell_addr, vals):
                        max_retries = 5
                        for i in range(max_retries):
                            try:
                                r, c = a1_to_rc(cell_addr)
                                worksheet.update(range_name=rc_to_a1(r, c + (start_day - 1)), values=[vals])
                                end_col = c + (start_day - 1) + len(vals) - 1
                                worksheet.format(f"{rc_to_a1(r, c + (start_day - 1))}:{rc_to_a1(r, end_col)}", NORMAL_FORMAT)
                                time.sleep(2)
                                return
                            except Exception as e:
                                if "429" in str(e) or "Quota exceeded" in str(e):
                                    wait_time = (2 ** i) + 2
                                    write_log(f"    レート制限(429)検知。{wait_time}秒待機して再試行します... ({i+1}/{max_retries})")
                                    time.sleep(wait_time)
                                else:
                                    write_log(f"    書き込みエラー: {e}"); break

                    if cancel_cell:        _upd(cancel_cell,        p_cancel)
                    if sum_cell:           _upd(sum_cell,           p_sum)
                    if complete_cell:      _upd(complete_cell,      p_complete)
                    if complete_cell_b:    _upd(complete_cell_b,    p_complete)
                    if next_month_cell:    _upd(next_month_cell,    p_next_month)
                    if next_month_cell_b:  _upd(next_month_cell_b,  p_next_month)
                    if rework_cur_cell:    _upd(rework_cur_cell,    p_rework_cur)
                    if rework_cur_cell_b:  _upd(rework_cur_cell_b,  p_rework_cur)
                    if rework_next_cell:   _upd(rework_next_cell,   p_rework_next)
                    if rework_next_cell_b: _upd(rework_next_cell_b, p_rework_next)
                    if pending_cell:       _upd(pending_cell,        p_pending)

                write_extra_contract_data("total", db_config["total_settings"].get("contract_cells", {}))
                for cat in db_config["category_settings"]:
                    write_extra_contract_data(cat["label"], cat.get("contract_cells", {}))

    write_log("=== 全処理完了 ===")

if __name__ == "__main__":
    main()