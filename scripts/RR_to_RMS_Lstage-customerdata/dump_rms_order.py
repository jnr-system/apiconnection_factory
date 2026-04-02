"""
RMS注文データの中身を確認するダンプスクリプト。
直近20件を取得し、skuInfoやselectedChoiceを一覧表示する。
給湯器本体（ガス種あり）の注文を探すのが目的。
"""
import base64
import requests
import json
import os
from datetime import datetime, timedelta
from pytz import timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

RMS_SERVICE_SECRET = os.environ["RMS_SERVICE_SECRET"]
RMS_LICENSE_KEY    = os.environ["RMS_LICENSE_KEY"]

DUMP_FILE = Path(__file__).parent / "rms_order_dump.json"

def main():
    token = base64.b64encode(f"{RMS_SERVICE_SECRET}:{RMS_LICENSE_KEY}".encode()).decode()
    headers = {
        "Authorization": f"ESA {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    jst = timezone("Asia/Tokyo")
    end_dt   = datetime.now(jst).strftime("%Y-%m-%dT%H:%M:%S+0900")
    start_dt = (datetime.now(jst) - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%S+0900")

    # 20件取得
    search_res = requests.post(
        "https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/",
        headers=headers,
        json={
            "dateType": 1,
            "startDatetime": start_dt,
            "endDatetime": end_dt,
            "PaginationRequestModel": {
                "requestRecordsAmount": 20,
                "requestPage": 1,
                "SortModelList": [{"sortColumn": 1, "sortDirection": 1}]
            }
        },
        timeout=30
    )
    search_res.raise_for_status()
    order_numbers = search_res.json().get("orderNumberList", [])

    if not order_numbers:
        print("注文が見つかりませんでした")
        return

    print(f"取得注文番号: {len(order_numbers)}件")

    # 詳細取得 version=7（merchantDefinedSkuId取得に必要）
    detail_res = requests.post(
        "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/",
        headers=headers,
        json={"orderNumberList": order_numbers, "version": "7"},
        timeout=30
    )
    detail_res.raise_for_status()
    order_data = detail_res.json().get("OrderModelList", [])

    if not order_data:
        print("詳細データが取得できませんでした")
        return

    # 全件JSONファイルに保存
    with open(DUMP_FILE, "w", encoding="utf-8") as f:
        json.dump(order_data, f, ensure_ascii=False, indent=2)

    print(f"ダンプ完了: {DUMP_FILE}")
    print("=" * 60)

    # 各注文の商品部分をコンソールに表示
    for order in order_data:
        print(f"\n■ 注文番号: {order.get('orderNumber')}")
        for pkg in order.get("PackageModelList", []):
            for item in pkg.get("ItemModelList", []):
                print(f"  --- 商品 ---")
                print(f"  itemName             : {item.get('itemName')}")
                print(f"  units                : {item.get('units')}")
                print(f"  selectedChoice       : {item.get('selectedChoice')}")
                for sku in item.get("SkuModelList", []):
                    print(f"  merchantDefinedSkuId : {sku.get('merchantDefinedSkuId')}")
                    print(f"  skuInfo              : {sku.get('skuInfo')}")

if __name__ == "__main__":
    main()
