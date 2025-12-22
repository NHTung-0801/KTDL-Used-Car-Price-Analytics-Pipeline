import sys
import os
# Fix đường dẫn import
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime
from crawler.utils import get_header

def crawl_bonbanh_brute_force(target_rows=100):
    # Bonbanh URL format: https://bonbanh.com/oto/page,2
    base_url = "https://bonbanh.com/oto"
    all_cars = []
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"data/raw/bonbanh_full_{timestamp}.csv"
    os.makedirs("data/raw", exist_ok=True)
    
    print(f"🚀 [BONBANH] Bắt đầu cào dữ liệu thô (Target: {target_rows} xe)...")
    print(f"   (Lưu ý: Bonbanh chặn bot rất gắt, code sẽ chạy chậm để an toàn)")
    
    current_page = 1
    total_scraped = 0
    consecutive_errors = 0
    
    while total_scraped < target_rows:
        # Cấu trúc link page của Bonbanh
        if current_page == 1:
            url = base_url
        else:
            url = f"{base_url}/page,{current_page}"
            
        print(f"   -> Đang cào Trang {current_page} (Đã có: {total_scraped} xe)")
        
        try:
            response = requests.get(url, headers=get_header(), timeout=15)
            
            if response.status_code != 200:
                print(f"   ⚠️ Lỗi kết nối {response.status_code}. Nghỉ 10s rồi thử lại...")
                time.sleep(10)
                consecutive_errors += 1
                if consecutive_errors > 3: break
                continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Tìm danh sách xe (Bonbanh thường dùng ul > li.car-item)
            listings = soup.find_all('li', class_='car-item')
            
            if not listings:
                print("   ⚠️ Không tìm thấy xe nào (Có thể bị chặn hoặc hết trang).")
                break
            
            consecutive_errors = 0
            
            for item in listings:
                car = {}
                
                # --- 1. TIÊU ĐỀ & URL ---
                # Thường nằm trong h3 > a
                title_tag = item.find('h3')
                if title_tag and title_tag.find('a'):
                    title_link = title_tag.find('a')
                    car['title'] = title_link.text.strip()
                    url_suffix = title_link.get('href')
                    car['url'] = "https://bonbanh.com/" + url_suffix if not url_suffix.startswith('http') else url_suffix
                else:
                    # Dự phòng nếu cấu trúc khác
                    car['title'] = item.get_text().split('\n')[0][:50]
                    car['url'] = url

                # --- 2. GIÁ ---
                # Bonbanh thường để giá trong tag <b> hoặc class cb3
                price_tag = item.find('div', class_='cb3')
                if price_tag:
                    car['price_raw'] = price_tag.text.strip()
                else:
                    # Tìm thẻ b chứa giá
                    price_b = item.find('b', itemprop='price')
                    car['price_raw'] = price_b.text.strip() if price_b else "0"
                
                # --- 3. LẤY TOÀN BỘ INFO (BRUTE FORCE) ---
                # Lấy hết text trong thẻ li, ngăn cách bằng dấu |
                full_text = item.get_text(separator=' | ', strip=True)
                
                # Làm sạch bớt xuống dòng thừa
                clean_full_text = " ".join(full_text.split())
                
                # Lưu vào info_raw để cleaning.py xử lý
                car['info_raw'] = clean_full_text
                
                # Metadata
                car['source'] = 'bonbanh'
                car['crawl_date'] = datetime.now().strftime("%Y-%m-%d")
                
                all_cars.append(car)
                total_scraped += 1
                
                if total_scraped >= target_rows: break
            
            # Lưu checkpoint
            if all_cars:
                pd.DataFrame(all_cars).to_csv(filename, index=False, encoding='utf-8-sig')

            current_page += 1
            # Bonbanh rất nhạy cảm, nên nghỉ lâu hơn (3-6 giây)
            time.sleep(random.uniform(3, 6))
            
        except Exception as e:
            print(f"❌ Lỗi trang {current_page}: {e}")
            consecutive_errors += 1
            time.sleep(5)

    print(f"\n✅ Xong Bonbanh! File raw: {filename}")
    print("👉 Bây giờ bạn hãy chạy 'python preprocessing/cleaning.py' để xem nó có lọc được không.")

if __name__ == "__main__":
    # Test trước 200 dòng
    crawl_bonbanh_brute_force(200)