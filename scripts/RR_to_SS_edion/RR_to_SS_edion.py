"""
【実行内容】
楽楽販売APIから問い合わせデータ（エディオン等）をCSV形式で取得し、
Googleスプレッドシートの「元データ管理シート」に反映（マージ）します。

レコードIDをキーとして、
- 既存レコード：スキップ（重複追記を防ぐ）
- 新規レコード：元データ管理シートに行を追加 → 正直屋管理シートのB列に「新規」をセット
"""
import requests
import csv
import io
import os
import json
import time
from datetime import datetime
from pathlib import Path
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# .env ファイルの自動読み込み（ローカル開発用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==============================================================================
# 設定エリア
# ==============================================================================

# ■ 楽楽販売の設定
RAKURAKU_DOMAIN    = "hntobias.rakurakuhanbai.jp"
RAKURAKU_TOKEN     = os.environ.get("RAKURAKU_TOKEN", "")
RAKURAKU_SCHEMA_ID = "101181"
RAKURAKU_SEARCH_ID = "107673"
RAKURAKU_LIST_ID   = "101473"

# ■ Googleスプレッドシートの設定
SPREADSHEET_KEY      = "1hBE66-67Se9c4FHXoILr7s5L-HDj5qjaqp4A6Z44kQ8"
MASTER_SHEET_NAME     = "元データ管理シート（マスタDB）"
SHOUJIKIYA_SHEET_NAME = "正直屋管理シート"
EDION_SHEET_NAME      = "エディオン確認用シート"

# ■ ログファイルの保存先
LOG_FILE_PATH = Path(__file__).parent / "execution_log.txt"

# ■ 手動管理対象（Pythonで上書きしない）列のリスト
#   ※ 新規追記時の初期値セットは対象外（既存行の更新のみ保護）
MANUAL_COLUMNS = [
    "新規フラグ",
    "先方確認フラグ",
    "ステータス",
    "見積作成日",
    "先方確認日",
    "備考",
    "見積金額",
    "見積書URL",
]

# ==============================================================================
# 共通関数: ログ出力
# ==============================================================================
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
# 処理開始
# ==============================================================================
def main():
    write_log("=== 問い合わせデータ連動処理を開始します ===")

    if not RAKURAKU_TOKEN:
        write_log("エラー: 環境変数 RAKURAKU_TOKEN が設定されていません。")
        return

    # ------------------------------------------------------------------
    # 1. 楽楽販売からCSVデータを取得
    # ------------------------------------------------------------------
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/api/csvexport/version/v1"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN,
    }
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "listId":     RAKURAKU_LIST_ID,
        "searchId":   RAKURAKU_SEARCH_ID,
        "limit":      5000,
    }

    write_log("楽楽販売からデータをダウンロード中...")
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=60)

        if res.status_code != 200:
            write_log(f"エラー: 楽楽販売APIが失敗しました ({res.status_code})")
            return

        try:
            csv_content = res.content.decode("cp932")
        except UnicodeDecodeError:
            csv_content = res.content.decode("utf-8", errors="ignore")

        f_obj   = io.StringIO(csv_content)
        reader  = csv.reader(f_obj)
        raw_data = list(reader)

        if not raw_data:
            write_log("データが空でした。処理を終了します。")
            return

        write_log(f"取得成功: {len(raw_data)} 行のデータを取得しました。")

        # ---- ヘッダーマップ作成 ----------------------------------------
        header = raw_data[0]
        header_map = {}
        for idx, col_name in enumerate(header):
            clean = col_name.strip('\ufeff').strip().replace('\n', '').replace('\r', '')
            header_map[clean] = idx

        # ---- スプシ書き込み列の定義（C列スタート）---------------------
        # C=レコードID, D=エディオン連携用手配番号, E=問い合わせ日,
        # F=顧客情報↴(結合ヘッダー), G=顧客名, H=顧客名カナ, I=顧客電話番号,
        # J=顧客メールアドレス, K=顧客住所,
        # L=商品情報↴(結合ヘッダー), M=現状商品品番(空), N=新規設置商品品番(空), O=ガス種,
        # P=エディオン情報↴(結合ヘッダー), Q=エディオン店舗名, R=エディオン担当者名,
        # S=工事日情報↴(結合ヘッダー), T=工事第１希望日, U=工事第２希望日, V=工事第３希望日,
        # W=写真URL(空)
        spreadsheet_headers = [
            "レコードID", "エディオン連携用手配番号", "問い合わせ日",
            "", "顧客名", "顧客名カナ", "顧客電話番号", "顧客メールアドレス", "顧客住所",
            "", "", "", "ガス種",
            "", "先方店舗名", "先方担当者名",
            "", "工事第１希望日", "工事第２希望日", "工事第３希望日",
            "スグフォームURL",
        ]

        # ---- CSV列名とスプシ列名のマッピング ---------------------------
        csv_to_ss_map = {
            "レコードID":             "記録ID",
            "エディオン連携用手配番号": "エディオン連携用手配番号",
            "問い合わせ日":           "新規問い合わせ日",
            "顧客名":                 "名前",
            "顧客名カナ":             "フリカナ",
            "顧客電話番号":           "電話番号_1",
            "顧客メールアドレス":     "メールアドレス_1",
            "ガス種":                 "ガスの種類",
            "先方店舗名":             "エディオン店舗名",
            "先方担当者名":           "エディオン担当者名",
            "工事第１希望日":         "第１希望日（エディオン）",
            "工事第２希望日":         "第２希望日（エディオン）",
            "工事第３希望日":         "第３希望日（エディオン）",
            "スグフォームURL":        "スグフォームURL（エディオン）",
        }

        address_csv_columns = [
            "エディオン案件用：郵便番号", "都道府県名", "市区名", "町村名・番地", "マンション・ビル名"
        ]

        # ---- 欠損列の警告 ----------------------------------------------
        all_required = list(csv_to_ss_map.values()) + address_csv_columns
        missing = [c for c in all_required if c not in header_map]
        if missing:
            write_log(f"⚠️ 警告: 以下の列がCSV内に見つかりませんでした: {missing}")
            write_log(f"ℹ️ 実際のCSV列名: {list(header_map.keys())}")

        # ---- データ整形 ------------------------------------------------
        data_list      = []  # スプシに書き込む行データ
        record_id_list = []  # フラグ更新用レコードID

        for row in raw_data[1:]:
            new_row   = []
            record_id = ""

            for ss_header in spreadsheet_headers:
                cell_val = ""

                if ss_header == "顧客住所":
                    parts = []
                    for col_name in address_csv_columns:
                        if col_name in header_map:
                            idx = header_map[col_name]
                            if idx < len(row) and row[idx]:
                                parts.append(row[idx])
                    cell_val = "".join(parts)

                elif ss_header in csv_to_ss_map:
                    csv_col = csv_to_ss_map[ss_header]
                    if csv_col in header_map:
                        idx = header_map[csv_col]
                        if idx < len(row):
                            cell_val = row[idx]

                clean_val = cell_val.strip().lstrip("'")
                new_row.append(clean_val)

                if ss_header == "レコードID":
                    record_id = clean_val

            data_list.append(new_row)
            record_id_list.append(record_id)

        write_log(f"データ整形完了: {len(data_list)} 件")

    except Exception as e:
        write_log(f"楽楽販売へのアクセス中にエラーが発生: {e}")
        return

    # ------------------------------------------------------------------
    # 2. Googleスプレッドシートに接続
    # ------------------------------------------------------------------
    write_log("Googleスプレッドシートに接続中...")
    try:
        scope    = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not json_str:
            write_log("エラー: 環境変数 GOOGLE_SERVICE_ACCOUNT_JSON が設定されていません。")
            return

        creds     = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(json_str), scope)
        client    = gspread.authorize(creds)
        workbook  = client.open_by_key(SPREADSHEET_KEY)

        # 元データ管理シート
        try:
            master_ws = workbook.worksheet(MASTER_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            write_log(f"シート '{MASTER_SHEET_NAME}' が見つからないため最初のシートを使用します。")
            master_ws = workbook.get_worksheet(0)

        # 正直屋管理シート
        shoujikiya_ws = workbook.worksheet(SHOUJIKIYA_SHEET_NAME)

        # エディオン確認用シート
        edion_ws = workbook.worksheet(EDION_SHEET_NAME)

    except Exception as e:
        write_log(f"スプレッドシート接続エラー: {e}")
        return

    # ------------------------------------------------------------------
    # 3. 既存レコードIDを取得して重複チェック用セットを作成
    #    元データ管理シートのC列（レコードID）= Excelの3列目
    # ------------------------------------------------------------------
    try:
        existing_values  = master_ws.get_all_values()  # 1〜2行目はヘッダー
        existing_row_count = len(existing_values)       # ヘッダー含む現在の行数

        # C列（index=2）からレコードIDをセットに格納（3行目以降がデータ）
        existing_record_ids = set()
        for row in existing_values[2:]:  # 3行目〜
            if len(row) > 2 and row[2]:
                existing_record_ids.add(str(row[2]).strip())

        write_log(f"既存レコード数: {len(existing_record_ids)} 件")

    except Exception as e:
        write_log(f"既存データ取得エラー: {e}")
        return

    # ------------------------------------------------------------------
    # 4. 新規レコードのみ抽出
    # ------------------------------------------------------------------
    new_data_list      = []
    new_record_id_list = []

    for row_data, record_id in zip(data_list, record_id_list):
        if record_id in existing_record_ids:
            write_log(f"スキップ（既存）: レコードID {record_id}")
        else:
            new_data_list.append(row_data)
            new_record_id_list.append(record_id)

    if not new_data_list:
        write_log("新規レコードはありませんでした。処理を終了します。")
        # 楽楽販売フラグ更新は不要なのでそのまま終了
        write_log("=== 完了 ===")
        return

    write_log(f"新規レコード: {len(new_data_list)} 件を追記します。")

    # ------------------------------------------------------------------
    # 5. 元データ管理シートに新規レコードを追記（C列〜）
    # ------------------------------------------------------------------
    try:
        next_row = max(existing_row_count + 1, 3)  # 最低でも3行目から開始

        master_ws.update(
            range_name=f"C{next_row}",
            values=new_data_list,
            value_input_option="USER_ENTERED",
        )
        write_log(f"元データ管理シート: {next_row} 行目〜 {len(new_data_list)} 件を追記しました。")

    except Exception as e:
        write_log(f"元データ管理シート追記エラー: {e}")
        return

    # ------------------------------------------------------------------
    # 6. 正直屋管理シートのC列（レコードID）を検索してB列に「新規」をセット
    # ------------------------------------------------------------------
    try:
        shoujikiya_values = shoujikiya_ws.get_all_values()

        # C列（index=2）のレコードID → 行番号（1始まり）のマップを作成（先頭ゼロ除去）
        shoujikiya_id_to_row = {}
        for i, row in enumerate(shoujikiya_values[1:], start=2):  # 2行目〜
            if len(row) > 2 and row[2]:
                shoujikiya_id_to_row[str(row[2]).strip().lstrip("0")] = i

        flag_success = 0
        flag_not_found = 0
        for record_id in new_record_id_list:
            if not record_id:
                continue
            if record_id.lstrip("0") in shoujikiya_id_to_row:
                target_row = shoujikiya_id_to_row[record_id.lstrip("0")]
                shoujikiya_ws.update(
                    range_name=f"B{target_row}",
                    values=[["新規"]],
                    value_input_option="USER_ENTERED",
                )
                flag_success += 1
                time.sleep(0.5)
            else:
                write_log(f"⚠️ 正直屋管理シートにレコードID {record_id} が見つかりませんでした。")
                flag_not_found += 1

        write_log(f"正直屋管理シート: B列「新規」セット完了 ({flag_success} 件) / 未発見 {flag_not_found} 件")

    except Exception as e:
        write_log(f"正直屋管理シート ステータスセットエラー: {e}")
        return

    # ------------------------------------------------------------------
    # 7. エディオン確認用シートのB列（レコードID）を検索してA列に「未確認」をセット
    # ------------------------------------------------------------------
    try:
        edion_values = edion_ws.get_all_values()

        # B列（index=1）のレコードID → 行番号のマップを作成（先頭ゼロ除去）
        edion_id_to_row = {}
        for i, row in enumerate(edion_values[1:], start=2):
            if len(row) > 1 and row[1]:
                edion_id_to_row[str(row[1]).strip().lstrip("0")] = i

        flag_success = 0
        flag_not_found = 0
        for record_id in new_record_id_list:
            if not record_id:
                continue
            if record_id.lstrip("0") in edion_id_to_row:
                target_row = edion_id_to_row[record_id.lstrip("0")]
                edion_ws.update(
                    range_name=f"A{target_row}",
                    values=[["未確認"]],
                    value_input_option="USER_ENTERED",
                )
                flag_success += 1
                time.sleep(0.5)
            else:
                write_log(f"⚠️ エディオン確認用シートにレコードID {record_id} が見つかりませんでした。")
                flag_not_found += 1

        write_log(f"エディオン確認用シート: A列「未確認」セット完了 ({flag_success} 件) / 未発見 {flag_not_found} 件")

    except Exception as e:
        write_log(f"エディオン確認用シート 更新エラー: {e}")
        return

    # ------------------------------------------------------------------
    # 9. 楽楽販売の「SS連携済みフラグ」を更新
    # ------------------------------------------------------------------
    write_log("楽楽販売のSS連携済みフラグを更新しています...")
    flag_success = 0
    flag_fail    = 0
    update_url     = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"
    update_headers = {"Content-Type": "application/json", "X-HD-apitoken": RAKURAKU_TOKEN}

    for record_id in new_record_id_list:
        if not record_id:
            continue
        update_payload = {
            "dbSchemaId": RAKURAKU_SCHEMA_ID,
            "keyId":      record_id,
            "values":     {"115979": "1"},  # SS連携済み（エディオン）
        }
        try:
            upd_res = requests.post(update_url, headers=update_headers, json=update_payload, timeout=10)
            if upd_res.status_code == 200:
                flag_success += 1
            else:
                write_log(f"[失敗] レコードID:{record_id} フラグ更新失敗: {upd_res.status_code} {upd_res.text}")
                flag_fail += 1
            time.sleep(1.0)
        except Exception as e:
            write_log(f"[例外] レコードID:{record_id} フラグ更新中にエラー: {e}")
            flag_fail += 1

    write_log(f"フラグ更新完了: 成功 {flag_success} 件 / 失敗 {flag_fail} 件")
    write_log("=== 完了: スプレッドシートの更新が終わりました ===")


if __name__ == "__main__":
    main()