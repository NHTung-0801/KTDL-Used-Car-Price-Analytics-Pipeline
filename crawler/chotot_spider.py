import requests
import pandas as pd
import time
import random
import os
import json
from datetime import datetime
from bs4 import BeautifulSoup

TARGET_ROWS = 10000
BASE_URL = "https://www.chotot.com/mua-ban-oto"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9",
}

def extract_next_data(html: str):
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None
    return json.loads(script.string)

def find_ads_anywhere(obj):
    """
    Quét đệ quy JSON để tìm list ads
    Ads hợp lệ: list[dict] có 'subject' và 'price'
    """
    results = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "ads" and isinstance(v, list):
                if v and isinstance(v[0], dict) and "subject" in v[0]:
                    results.extend(v)
            else:
                results.extend(find_ads_anywhere(v))

    elif isinstance(obj, list):
        for item in obj:
            results.extend(find_ads_anywhere(item))

    return results


def crawl_chotot_html():
    print(f"🚀 Bắt đầu cào Chotot HTML (Mục tiêu: {TARGET_ROWS})")

    os.makedirs("data/raw", exist_ok=True)
    filename = f"data/raw/chotot_full_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    all_cars = []
    page = 1

    while len(all_cars) < TARGET_ROWS:
        print(f"➡️ Page {page}")

        r = requests.get(
            f"{BASE_URL}?page={page}",
            headers=HEADERS,
            timeout=20
        )

        if r.status_code != 200:
            print(f"⚠️ HTTP {r.status_code}")
            break

        next_data = extract_next_data(r.text)
        if not next_data:
            print("❌ Không tìm thấy __NEXT_DATA__")
            break

        ads = find_ads_anywhere(next_data)

        if not ads:
            print("⚠️ Trang này không có ads")
            break

        print(f"   ✅ Tìm thấy {len(ads)} xe")

        for item in ads:
            car = {
                "title": item.get("subject", ""),
                "price_raw": str(item.get("price", "")),
                "info_raw": json.dumps(item, ensure_ascii=False),
                "url": f"https://www.chotot.com/{item.get('list_id')}.htm",
                "source": "chotot",
                "crawl_date": datetime.now().strftime("%Y-%m-%d"),
            }
            all_cars.append(car)

        if len(all_cars) % 300 == 0:
            pd.DataFrame(all_cars).to_csv(
                filename, index=False, encoding="utf-8-sig"
            )
            print(f"💾 Checkpoint {len(all_cars)}")

        page += 1
        time.sleep(random.uniform(1.2, 2.5))

    df = pd.DataFrame(all_cars)
    df.to_csv(filename, index=False, encoding="utf-8-sig")

    print(f"✅ HOÀN TẤT: {len(df)} xe")
    print(f"📁 File: {filename}")
    print("👉 File sẵn sàng cho cleaning.py")


if __name__ == "__main__":
    crawl_chotot_html()
