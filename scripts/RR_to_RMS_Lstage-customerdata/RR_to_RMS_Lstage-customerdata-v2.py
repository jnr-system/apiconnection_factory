"""
【テスト内容】
既存のレコード更新APIに明細項目（details）の複数行追加を組み込むテスト。
RMSへの参照は不要。手動で指定したkeyIdのレコードに対して
ヘッダ項目の更新 + 明細複数行の追加を同時に行う。

【確認ポイント】
1. 既存レコードのヘッダ項目を更新できるか
2. 明細（details）を複数行同時に追加できるか
3. DBリンク項目（115996）＋getSubordinate=1 で従属項目が自動取得されるか
"""

import requests
import json
import os
import time
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==============================================================================
# 設定エリア
# ==============================================================================

RAKURAKU_DOMAIN    = "hntobias.rakurakuhanbai.jp"
RAKURAKU_TOKEN     = os.environ["RAKURAKU_TOKEN"]

RAKURAKU_SCHEMA_ID = "101451"  # テスト対象のDBスキーマID（発注DBのID）

# ★ テスト対象のレコード（楽楽販売で手動作成したレコードのkeyId）
TEST_KEY_ID = "000000002"  # ← 先ほど登録されたレコードのkeyIdを使用

# ★ テスト用の商品キー（実際に商品マスタに存在するもの）
TEST_SHOHIN_KEY_1 = "000000794"
TEST_SHOHIN_KEY_2 = "000002505"

LOG_FILE_PATH = Path(__file__).parent / "test_update_meisai_log.txt"

# ==============================================================================
# 共通関数
# ==============================================================================

def write_log(message: str):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{now_str}] {message}"
    print(log_msg)
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(log_msg + "\n")

def get_headers() -> dict:
    return {
        "Content-Type": "application/json; charset=utf-8",
        "X-HD-apitoken": RAKURAKU_TOKEN
    }

# ==============================================================================
# レコード参照（更新前の状態確認）
# ==============================================================================

def view_record(key_id: str) -> dict:
    """更新前後の状態確認用"""
    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/view/version/v1"
    payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "keyId": key_id,
        "responseType": "1"  # 項目タイプ名で出力
    }
    try:
        res = requests.post(url, headers=get_headers(), json=payload, timeout=15)
        if res.status_code == 200:
            data = res.json()
            write_log(f"レコード参照結果:\n{json.dumps(data.get('items', {}), ensure_ascii=False, indent=2)}")
            return data.get("items", {})
        else:
            write_log(f"参照失敗: {res.status_code} {res.text}")
            return {}
    except Exception as e:
        write_log(f"参照エラー: {e}")
        return {}

# ==============================================================================
# メインテスト: ヘッダ更新 + 明細複数行追加
# ==============================================================================

def test_update_with_meisai(key_id: str):
    """
    既存レコードのヘッダ項目を更新しながら
    明細（details）を複数行追加するテスト。

    仕様書p.46より:
    - updateDetailKeyId を指定しない場合、detailKeyなしの行は末尾に新規追加される
    - getSubordinate=1 でDBリンクの従属項目を自動取得
    """
    write_log(f"=== ヘッダ更新 + 明細複数行追加テスト (keyId={key_id}) ===")

    url = f"https://{RAKURAKU_DOMAIN}/mspy4wa/apirecord/update/version/v1"

    # 既存コード（RR_to_RMS）の更新項目 + 明細追加を組み合わせる
    update_payload = {
        "dbSchemaId": RAKURAKU_SCHEMA_ID,
        "keyId": key_id,
        "getSubordinate": "1",          # DBリンクの従属項目を自動取得
        "updateDetailKeyId": "detailKey",  # 明細キーで行を特定（なければ新規追加）
        "values": {
            # ── ヘッダ項目の更新（既存コードの項目）──
            # 実際の項目IDに合わせて書き換えてください
            # "113811": "06-1234-5678",   # 電話番号
            # "113772": "50000",           # 合計金額
            # "113095": "5300001",         # 郵便番号
            # "113096": "大阪府",          # 都道府県
            # "113097": "大阪市",          # 市区町村
            # "113098": "1-1-1",           # 番地
            # "113089": "06-9999-9999",    # 配送先電話番号
            # "113073": "20260410",        # 配送希望日

            # ── 明細項目の追加（複数行）──
            "details": [
                {
                    # detailKeyなし → 末尾に新規行として追加
                    "115996": TEST_SHOHIN_KEY_1,  # 商品選択（DBリンク）
                    "115998": "2",                # 個数
                    # 従属項目（商品名・原価・メーカー名など）はgetSubordinate=1で自動取得
                },
                {
                    "115996": TEST_SHOHIN_KEY_2,  # 別の商品
                    "115998": "1",                # 個数
                },
            ]
        }
    }

    write_log(f"送信URL: {url}")
    write_log(f"送信ペイロード:\n{json.dumps(update_payload, ensure_ascii=False, indent=2)}")

    try:
        res = requests.post(url, headers=get_headers(), json=update_payload, timeout=30)
        write_log(f"レスポンスコード: {res.status_code}")

        try:
            res_json = res.json()
            write_log(f"レスポンス:\n{json.dumps(res_json, ensure_ascii=False, indent=2)}")
        except Exception:
            write_log(f"レスポンス（生テキスト）: {res.text}")
            return False

        if res.status_code == 200 and res_json.get("status") == "success":
            write_log(f"✅ 更新成功！ keyId={res_json.get('items', {}).get('keyId')}")
            write_log("→ 楽楽販売の画面で以下を確認してください：")
            write_log("   - 明細が追加されているか")
            write_log("   - 商品名・原価・メーカー名などの従属項目が自動セットされているか")
            return True
        else:
            errors = res_json.get("errors", {})
            write_log(f"❌ 更新失敗: {errors}")
            _analyze_error(errors)
            return False

    except Exception as e:
        write_log(f"❌ 例外発生: {e}")
        import traceback
        traceback.print_exc()
        return False


# ==============================================================================
# エラー分析
# ==============================================================================

def _analyze_error(errors: dict):
    code = errors.get("code", "")
    msg  = errors.get("msg", "")
    desc = errors.get("description", [])

    write_log(f"  エラーコード: {code}, メッセージ: {msg}")

    for d in desc:
        for h in d.get("header", []):
            write_log(f"  [ヘッダ項目エラー] 項目ID={h.get('name')} 値={h.get('value')} → {h.get('msg')}")
        detail_errors = d.get("detail", {})
        for row_no, row_errs in detail_errors.items():
            for e in row_errs:
                write_log(f"  [明細{row_no}行目エラー] 項目ID={e.get('name')} 値={e.get('value')} → {e.get('msg')}")

    if code == "100":
        write_log("  💡 商品キーが存在しないか、必須項目が不足している可能性があります")
    elif code == "1":
        write_log("  💡 認証エラー: RAKURAKU_TOKEN を確認してください")


# ==============================================================================
# メイン
# ==============================================================================

def main():
    write_log("=" * 60)
    write_log("楽楽販売 レコード更新 + 明細複数行追加テスト")
    write_log("=" * 60)
    write_log(f"対象keyId: {TEST_KEY_ID}")
    write_log(f"追加する商品キー: {TEST_SHOHIN_KEY_1}, {TEST_SHOHIN_KEY_2}")

    # 1. 更新前の状態確認
    write_log("\n--- 更新前のレコード状態 ---")
    view_record(TEST_KEY_ID)

    time.sleep(1)

    # 2. ヘッダ更新 + 明細追加
    result = test_update_with_meisai(TEST_KEY_ID)

    time.sleep(1)

    # 3. 更新後の状態確認
    if result:
        write_log("\n--- 更新後のレコード状態 ---")
        view_record(TEST_KEY_ID)

    write_log("=" * 60)
    write_log("テスト完了")
    write_log("=" * 60)


if __name__ == "__main__":
    main()