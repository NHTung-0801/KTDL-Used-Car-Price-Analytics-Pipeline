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
from tqdm import tqdm

def crawl_bonbanh_brute_force(target_rows=10000):
    """
    Crawl dữ liệu từ bonbanh.com với target 10,000 xe
    
    Args:
        target_rows: Số lượng xe cần crawl (mặc định 10000)
    """
    # Bonbanh URL format: https://bonbanh.com/oto/page,2
    base_url = "https://bonbanh.com/oto"
    all_cars = []
<<<<<<< HEAD
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"data/raw/bonbanh_full_{timestamp}.csv"
    os.makedirs("data/raw", exist_ok=True)
=======

    output_dir = os.path.join(root_dir, 'data', 'raw')
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = os.path.join(output_dir, f"bonbanh_full_{timestamp}.csv")
>>>>>>> daca89c9e2d6901ba83017287808cf9dcda97f35
    
    print(f"🚀 [BONBANH] Bắt đầu cào dữ liệu thô (Target: {target_rows:,} xe)...")
    print(f"   (Lưu ý: Bonbanh chặn bot rất gắt, code sẽ chạy chậm để an toàn)")
    print(f"   📁 File sẽ được lưu tại: {filename}\n")
    
    current_page = 1
    total_scraped = 0
    consecutive_errors = 0
    max_consecutive_errors = 5
    empty_pages_count = 0
    max_empty_pages = 3
    
    # Progress bar
    pbar = tqdm(total=target_rows, desc="Đang crawl", unit="xe", ncols=100)
    
    while total_scraped < target_rows:
        # Cấu trúc link page của Bonbanh
        if current_page == 1:
            url = base_url
        else:
            url = f"{base_url}/page,{current_page}"
            
        try:
            # Thêm delay ngẫu nhiên để tránh bị chặn
            delay = random.uniform(3, 7)
            time.sleep(delay)
            
            response = requests.get(url, headers=get_header(), timeout=20)
            
            if response.status_code != 200:
                print(f"\n   ⚠️ Lỗi kết nối {response.status_code} tại trang {current_page}. Nghỉ 15s rồi thử lại...")
                time.sleep(15)
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    print(f"\n   ❌ Đã có {max_consecutive_errors} lỗi liên tiếp. Dừng crawler.")
                    break
                continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Tìm danh sách xe với nhiều selector dự phòng
            listings = []
            
            # Thử nhiều cách tìm listings
<<<<<<< HEAD
            listings = soup.find_all('li', class_='car-item')
            if not listings:
                listings = soup.find_all('div', class_='car-item')
=======
            listings = soup.find_all('li', class_='car-detail')
            if not listings:
                listings = soup.find_all('div', class_='car-detail')
>>>>>>> daca89c9e2d6901ba83017287808cf9dcda97f35
            if not listings:
                listings = soup.find_all('li', class_=lambda x: x and 'car' in x.lower())
            if not listings:
                listings = soup.find_all('div', class_=lambda x: x and 'car' in x.lower())
            if not listings:
                # Thử tìm trong các container phổ biến
                containers = soup.find_all(['ul', 'div'], class_=lambda x: x and ('list' in x.lower() or 'grid' in x.lower()))
                for container in containers:
                    listings = container.find_all(['li', 'div'], recursive=False)
                    if listings:
                        break
            
            if not listings:
                empty_pages_count += 1
                print(f"\n   ⚠️ Trang {current_page}: Không tìm thấy xe nào (Có thể bị chặn hoặc hết trang).")
                if empty_pages_count >= max_empty_pages:
                    print(f"   ❌ Đã có {max_empty_pages} trang trống liên tiếp. Dừng crawler.")
                break
                current_page += 1
                continue
            
            # Reset counters khi tìm thấy dữ liệu
            consecutive_errors = 0
            empty_pages_count = 0
            
            page_cars_count = 0
            
            for item in listings:
                if total_scraped >= target_rows:
                    break
                    
                car = {}
                
                # --- 1. TIÊU ĐỀ & URL ---
                # Thử nhiều cách tìm title
                title_tag = None
                title_link = None
                
                # Cách 1: h3 > a
                title_tag = item.find('h3')
                if title_tag:
                    title_link = title_tag.find('a')
                
                # Cách 2: a với class title
                if not title_link:
                    title_link = item.find('a', class_=lambda x: x and 'title' in x.lower())
                
                # Cách 3: a đầu tiên
                if not title_link:
                    title_link = item.find('a')
                
                if title_link:
                    car['title'] = title_link.text.strip()
                    url_suffix = title_link.get('href', '')
                    if url_suffix:
                        if url_suffix.startswith('http'):
                            car['url'] = url_suffix
                        elif url_suffix.startswith('/'):
                            car['url'] = "https://bonbanh.com" + url_suffix
                        else:
                            car['url'] = "https://bonbanh.com/" + url_suffix
                    else:
                        car['url'] = url
                else:
                    # Dự phòng: lấy text đầu tiên làm title
                    text_parts = item.get_text(strip=True).split('\n')
                    car['title'] = text_parts[0][:100] if text_parts else "Unknown"
                    car['url'] = url

                # --- 2. GIÁ ---
                # Thử nhiều cách tìm giá
                price_text = "0"
                
                # Cách 1: div class cb3
                price_tag = item.find('div', class_='cb3')
                if price_tag:
                    price_text = price_tag.text.strip()
                else:
                    # Cách 2: b với itemprop='price'
                    price_b = item.find('b', itemprop='price')
                    if price_b:
                        price_text = price_b.text.strip()
                    else:
                        # Cách 3: span/div với class chứa 'price'
                        price_elem = item.find(['span', 'div', 'p'], class_=lambda x: x and 'price' in x.lower())
                        if price_elem:
                            price_text = price_elem.text.strip()
                        else:
                            # Cách 4: tìm text chứa "tỷ", "triệu", "VNĐ"
                            all_text = item.get_text()
                            if any(keyword in all_text for keyword in ['tỷ', 'triệu', 'VNĐ', 'đồng']):
                                # Lấy đoạn text có chứa giá
                                lines = all_text.split('\n')
                                for line in lines:
                                    if any(keyword in line for keyword in ['tỷ', 'triệu', 'VNĐ']):
                                        price_text = line.strip()
                                        break
                
                car['price_raw'] = price_text
                
                # --- 3. LẤY TOÀN BỘ INFO (BRUTE FORCE) ---
                # Lấy hết text trong item, ngăn cách bằng dấu |
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
                page_cars_count += 1
                pbar.update(1)
            
            # Lưu checkpoint sau mỗi trang để tránh mất dữ liệu
            if all_cars:
                try:
                    df = pd.DataFrame(all_cars)
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                except Exception as save_error:
                    print(f"\n   ⚠️ Lỗi khi lưu file: {save_error}")
            
            if page_cars_count > 0:
                pbar.set_postfix({
                    'Trang': current_page,
                    'Xe/trang': page_cars_count,
                    'Tổng': total_scraped
                })

            current_page += 1
            
        except requests.exceptions.Timeout:
            print(f"\n   ⚠️ Timeout tại trang {current_page}. Nghỉ 10s rồi thử lại...")
            consecutive_errors += 1
            time.sleep(10)
            if consecutive_errors >= max_consecutive_errors:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"\n   ⚠️ Lỗi kết nối tại trang {current_page}: {e}. Nghỉ 15s rồi thử lại...")
            consecutive_errors += 1
            time.sleep(15)
            if consecutive_errors >= max_consecutive_errors:
                break
            
        except Exception as e:
            print(f"\n   ❌ Lỗi không xác định tại trang {current_page}: {e}")
            consecutive_errors += 1
            time.sleep(10)
            if consecutive_errors >= max_consecutive_errors:
                break
    
    pbar.close()
    
    # Lưu file cuối cùng
    if all_cars:
        try:
            df = pd.DataFrame(all_cars)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n✅ Hoàn thành! Đã crawl được {len(all_cars):,} xe")
            print(f"📁 File đã lưu tại: {filename}")
        except Exception as e:
            print(f"\n❌ Lỗi khi lưu file cuối cùng: {e}")
    else:
        print(f"\n⚠️ Không có dữ liệu nào được crawl!")
    
    print(f"\n👉 Bây giờ bạn hãy chạy 'python preprocessing/cleaning.py' để làm sạch dữ liệu.")

if __name__ == "__main__":
    # Crawl 10,000 xe
<<<<<<< HEAD
    crawl_bonbanh_brute_force(10000)
=======
    crawl_bonbanh_brute_force(100)
>>>>>>> daca89c9e2d6901ba83017287808cf9dcda97f35
