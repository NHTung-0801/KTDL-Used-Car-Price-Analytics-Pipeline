# Code cào Oto.com.vn (Team B)
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from crawler.utils import get_header

def crawl_otocomvn(pages=5):
    base_url = "https://oto.com.vn/mua-ban-xe"
    all_cars = []
    
    for p in range(1, pages + 1):
        # Cấu trúc link phân trang của Oto.com.vn: /p2, /p3...
        url = f"{base_url}/p{p}"
        print(f"Dang cao: {url}")
        
        try:
            # Phải có headers nếu không sẽ bị chặn (403 Forbidden)
            response = requests.get(url, headers=get_header())
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Lưu ý: Class này có thể thay đổi, Team C cần F12 kiểm tra lại
            # Thường là 'item-car' hoặc một div chứa thông tin xe
            listings = soup.find_all('div', class_='item-car') 
            
            for item in listings:
                car = {}
                # Lấy tên xe
                title_tag = item.find('h3', class_='title')
                if title_tag:
                    car['title'] = title_tag.text.strip()
                    car['url'] = "https://oto.com.vn" + title_tag.find('a')['href']
                
                # Lấy giá
                price_tag = item.find('p', class_='price')
                car['price_raw'] = price_tag.text.strip() if price_tag else '0'
                
                # Lấy thông tin ODO, Năm sx (thường nằm trong ul/li)
                info_tag = item.find('ul', class_='info')
                if info_tag:
                    infos = info_tag.find_all('li')
                    # Cần xử lý logic lấy từng li ở đây (Năm, ODO, Nơi bán)
                    car['info_raw'] = " | ".join([i.text.strip() for i in infos])
                
                car['source'] = 'otocomvn'
                all_cars.append(car)
                
            time.sleep(2) # Nghỉ 2s để không bị chặn
            
        except Exception as e:
            print(f"Loi tai trang {p}: {e}")
            
    return pd.DataFrame(all_cars)
