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

def crawl_otocomvn_brute_force(target_rows=5000):
    base_url = "https://oto.com.vn/mua-ban-xe"
    all_cars = []
    
    output_dir = os.path.join(root_dir, 'data', 'raw')
    os.makedirs("data/raw", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = os.path.join(output_dir, f"otocomvn_full_{timestamp}.csv")

    
    print(f"🚀 Bắt đầu chế độ 'VƠ VÉT TẤT CẢ' (Lấy toàn bộ text hiển thị)...")
    print(f"💾 File sẽ lưu tại: {filename}")
    
    current_page = 1
    total_scraped = 0
    consecutive_errors = 0
    
    while total_scraped < target_rows:
        url = f"{base_url}/p{current_page}"
        print(f"   -> Đang cào Trang {current_page} (Đã có: {total_scraped} xe)")
        
        try:
            response = requests.get(url, headers=get_header(), timeout=15)
            if response.status_code != 200:
                print(f"   ⚠️ Lỗi kết nối: {response.status_code}. Thử lại sau 5s...")
                time.sleep(5)
                consecutive_errors += 1
                if consecutive_errors > 5: break
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 1. Tìm khung chứa tin đăng
            listings = soup.find_all('div', class_='item-car')
            if not listings:
                listings = soup.find_all('div', class_='box-listing-car')
            
            if not listings:
                print("   ⚠️ Không tìm thấy xe nào (Hết trang hoặc bị chặn).")
                break
            
            consecutive_errors = 0 
            
            for item in listings:
                car = {}
                
                # --- A. CÁC CỘT CƠ BẢN (Cố gắng lấy riêng cho tiện) ---
                # Tiêu đề
                title_tag = item.find('a', class_='title') or item.find('h3', class_='title')
                car['title'] = title_tag.text.strip() if title_tag else "Unknown"
                
                # URL
                if title_tag and title_tag.name == 'a':
                    link = title_tag.get('href')
                elif title_tag and title_tag.find('a'):
                    link = title_tag.find('a').get('href')
                else:
                    link = ""
                car['url'] = "https://oto.com.vn" + link if link and not link.startswith('http') else link

                # Giá (Lấy riêng để dễ nhìn, nhưng cũng sẽ có trong info_raw)
                price_tag = item.find('span', class_='price') or item.find('p', class_='price')
                car['price_raw'] = price_tag.text.strip() if price_tag else "0"
                
                # --- B. INFO_RAW: LẤY HẾT MỌI THỨ (BRUTE FORCE) ---
                # Lệnh get_text(separator=' | ') sẽ lấy toàn bộ chữ trong thẻ div,
                # bao gồm cả năm, mô tả, địa điểm, người bán, icon... 
                # cách nhau bằng dấu gạch đứng " | "
                full_text = item.get_text(separator=' | ', strip=True)
                
                # Loại bỏ các ký tự xuống dòng thừa thãi
                clean_full_text = " ".join(full_text.split())
                
                car['info_raw'] = clean_full_text
                # ----------------------------------------------------

                car['source'] = 'otocomvn'
                car['crawl_date'] = datetime.now().strftime("%Y-%m-%d")
                
                all_cars.append(car)
                total_scraped += 1
                
                if total_scraped >= target_rows: break
            
            # Lưu Checkpoint
            if all_cars and (current_page % 5 == 0 or total_scraped >= target_rows):
                df = pd.DataFrame(all_cars)
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"   💾 [Checkpoint] Đã lưu {len(df)} dòng.")

            current_page += 1
            time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"❌ Lỗi trang {current_page}: {e}")
            consecutive_errors += 1
            time.sleep(5)

    print(f"✅ Xong! File mới tại: {filename}")
    print("👉 File này chắc chắn cột info_raw sẽ đầy ắp chữ. Bạn hãy chạy lại Cleaning để lọc sau.")

if __name__ == "__main__":
    crawl_otocomvn_brute_force(10000)