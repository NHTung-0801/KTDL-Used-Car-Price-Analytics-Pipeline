import pandas as pd
import glob
import os
import re
from datetime import datetime

# ==============================================================================
# 1. CÁC HÀM TRÍCH XUẤT (EXTRACTORS)
# ==============================================================================

def clean_price(price_raw):
    """
    Xử lý giá tiền từ text sang số nguyên (VNĐ)
    Hỗ trợ: 'tỉ', 'tỷ', 'triệu', 'VNĐ', 'đồng'
    
    Returns:
        int: Giá tiền tính bằng VNĐ, hoặc None nếu không parse được
    """
    if pd.isna(price_raw): 
        return None
    
    p_str = str(price_raw).lower().strip()
    
    # Loại bỏ các trường hợp không có giá
    if any(keyword in p_str for keyword in ['liên hệ', 'thỏa thuận', 'giá', 'call', 'contact']):
        if 'triệu' not in p_str and 'tỷ' not in p_str and 'tỉ' not in p_str:
            return None
    
    # Chuẩn hóa: thay 'tỉ' thành 'tỷ'
    p_str = p_str.replace('tỉ', 'tỷ')
    # Loại bỏ dấu chấm, phẩy
    p_str = p_str.replace('.', '').replace(',', '').replace(' ', '')
    
    try:
        # Xử lý trường hợp: "4 Tỷ 279 Triệu"
        if 'tỷ' in p_str and 'triệu' in p_str:
            parts = p_str.split('tỷ')
            ty_match = re.search(r'(\d+)', parts[0])
            trieu_match = re.search(r'(\d+)', parts[1])
            
            ty = int(ty_match.group(1)) if ty_match else 0
            trieu = int(trieu_match.group(1)) if trieu_match else 0
            
            return int(ty * 1_000_000_000 + trieu * 1_000_000)
        
        # Xử lý trường hợp chỉ có "Tỷ"
        elif 'tỷ' in p_str:
            match = re.search(r'(\d+)', p_str)
            if match:
                ty = int(match.group(1))
                return int(ty * 1_000_000_000)
        
        # Xử lý trường hợp chỉ có "Triệu"
        elif 'triệu' in p_str:
            match = re.search(r'(\d+)', p_str)
            if match:
                trieu = int(match.group(1))
                return int(trieu * 1_000_000)
        
        # Xử lý số thuần (đã là VNĐ)
        else:
            # Tìm tất cả số trong chuỗi, lấy số lớn nhất
            numbers = re.findall(r'\d+', p_str)
            if numbers:
                # Lấy số lớn nhất (thường là giá)
                max_num = max(numbers, key=len)
                return int(max_num)
        
        return None
    except Exception:
        return None


def extract_year_smart(row):
    """
    Tìm năm sản xuất từ title và info_raw
    Ưu tiên tìm trong title trước
    
    Returns:
        int: Năm sản xuất (YYYY), hoặc None nếu không tìm thấy
    """
    # 1. Tìm trong title (thường có dạng: "2014 - Mazda CX5...")
    title = str(row.get('title', ''))
    year_match = re.search(r'\b(19|20)\d{2}\b', title)
    if year_match:
        year = int(year_match.group(0))
        current_year = datetime.now().year
        if 1990 <= year <= current_year + 1:
            return year
    
    # 2. Tìm trong info_raw
    info = str(row.get('info_raw', ''))
    year_match = re.search(r'\b(19|20)\d{2}\b', info)
    if year_match:
        year = int(year_match.group(0))
        current_year = datetime.now().year
        if 1990 <= year <= current_year + 1:
            return year
    
    return None


def extract_brand_model_smart(title):
    """
    Tách Brand và Model từ tiêu đề
    
    Ví dụ: "2014 - Mazda CX5 2.0 AT" -> brand="Mazda", model="CX5"
    
    Returns:
        tuple: (brand, model)
    """
    if pd.isna(title):
        return "Other", "Other"
    
    # Bỏ năm và dấu gạch ngang ở đầu: "2014 - " hoặc "2014-"
    clean_title = re.sub(r'^(19|20)\d{2}\s*[-–]\s*', '', str(title).strip())
    
    # Bỏ các từ không cần thiết ở đầu
    clean_title = re.sub(r'^(xe\s+(cũ|mới|đã|sử dụng))\s*', '', clean_title, flags=re.IGNORECASE)
    
    parts = clean_title.split()
    if not parts:
        return "Other", "Other"
    
    # Danh sách hãng xe phổ biến (mở rộng)
    brands_list = [
        'toyota', 'hyundai', 'kia', 'mazda', 'honda', 'ford', 'mercedes', 'bmw', 
        'audi', 'vinfast', 'mitsubishi', 'nissan', 'suzuki', 'lexus', 'porsche', 
        'land rover', 'mg', 'peugeot', 'volvo', 'subaru', 'isuzu', 'chevrolet',
        'renault', 'vw', 'volkswagen', 'mini', 'jaguar', 'infiniti', 'acura',
        'genesis', 'cadillac', 'lincoln', 'bentley', 'rolls-royce', 'maserati',
        'ferrari', 'lamborghini', 'mclaren', 'tesla', 'fiat', 'opel', 'skoda',
        'seat', 'dacia', 'geely', 'haval', 'great wall', 'chery', 'byd'
    ]
    
    brand = "Other"
    model = "Other"
    
    # Tìm brand (thường là từ đầu tiên hoặc từ đầu tiên + từ thứ hai)
    found_brand = False
    
    # Thử 2 từ đầu (cho "Land Rover", "Great Wall")
    if len(parts) >= 2:
        two_words = f"{parts[0].lower()} {parts[1].lower()}"
        for b in brands_list:
            if b in two_words:
                if b == 'bmw' or b == 'mg' or b == 'vw':
                    brand = b.upper()
                elif b == 'land rover':
                    brand = 'Land Rover'
                elif b == 'great wall':
                    brand = 'Great Wall'
                else:
                    brand = b.title()
                found_brand = True
                # Model là từ thứ 3 trở đi
                if len(parts) > 2:
                    model = ' '.join(parts[2:4])  # Lấy 2 từ đầu của model
                break
    
    # Nếu chưa tìm thấy, thử 1 từ đầu
    if not found_brand and len(parts) > 0:
        first_word = parts[0].lower()
        for b in brands_list:
            if b in first_word or first_word in b:
                if b == 'bmw' or b == 'mg' or b == 'vw':
                    brand = b.upper()
                else:
                    brand = b.title()
                found_brand = True
                # Model là từ thứ 2 trở đi
                if len(parts) > 1:
                    model = parts[1]  # Lấy từ đầu tiên của model
                break
    
    # Nếu vẫn chưa tìm thấy, lấy từ đầu tiên làm brand (heuristic)
    if not found_brand and len(parts) > 0:
        brand = parts[0].title()
        if len(parts) > 1:
            model = parts[1]
    
    # Làm sạch model (bỏ số, ký tự đặc biệt không cần thiết)
    if model != "Other":
        model = re.sub(r'^\d+\.?\d*\s*', '', model)  # Bỏ số ở đầu
        model = model.strip()
    
    return brand, model


def extract_mileage_smart(text):
    """
    Tìm số km đã đi (mileage/odo)
    
    Returns:
        int: Số km, hoặc 0 nếu không tìm thấy
    """
    if pd.isna(text):
        return 0
    
    s = str(text).lower()
    # Loại bỏ dấu chấm, phẩy trong số
    s = s.replace('.', '').replace(',', '')
    
    # Tìm pattern: số + "km" hoặc "v km" (ví dụ: "110,000 km" hoặc "11v km")
    patterns = [
        r'(\d+)\s*v\s*km',  # "11v km"
        r'(\d+)\s*km',       # "110000 km"
        r'đã\s*đi\s*(\d+)',  # "đã đi 110000"
        r'odo[:\s]*(\d+)',   # "odo: 110000"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, s)
        if match:
            try:
                km = int(match.group(1))
                # Nếu là "v km" (ví dụ: 11v = 110,000), nhân với 10000
                if 'v' in match.group(0).lower():
                    km = km * 10000
                return km
            except:
                continue
    
    return 0


def extract_fuel(text):
    """
    Tìm loại nhiên liệu
    
    Returns:
        str: Loại nhiên liệu (Xăng, Dầu, Điện, Hybrid, Unknown)
    """
    if pd.isna(text):
        return "Unknown"
    
    s = str(text).lower()
    
    # Ưu tiên: Điện > Hybrid > Dầu > Xăng
    if any(keyword in s for keyword in ['điện', 'ev', 'electric', 'pin']):
        return 'Điện'
    if any(keyword in s for keyword in ['hybrid', 'hev', 'phev']):
        return 'Hybrid'
    if any(keyword in s for keyword in ['dầu', 'diesel']):
        return 'Dầu'
    if any(keyword in s for keyword in ['xăng', 'petrol', 'gasoline', 'benzin']):
        return 'Xăng'
    
    return "Unknown"


def extract_color(text):
    """
    Tìm màu sắc xe
    
    Returns:
        str: Màu xe, hoặc "Unknown"
    """
    if pd.isna(text):
        return "Unknown"
    
    s = str(text).lower()
    
    colors_map = {
        'trắng': 'Trắng', 'trang': 'Trắng',
        'đen': 'Đen', 'den': 'Đen',
        'đỏ': 'Đỏ', 'do': 'Đỏ', 'red': 'Đỏ',
        'bạc': 'Bạc', 'bac': 'Bạc', 'silver': 'Bạc',
        'xám': 'Xám', 'xam': 'Xám', 'ghi': 'Xám', 'gray': 'Xám', 'grey': 'Xám',
        'nâu': 'Nâu', 'nau': 'Nâu', 'brown': 'Nâu',
        'vàng': 'Vàng', 'vang': 'Vàng', 'yellow': 'Vàng',
        'cam': 'Cam', 'orange': 'Cam',
        'xanh': 'Xanh', 'blue': 'Xanh', 'green': 'Xanh',
        'xanh dương': 'Xanh dương', 'xanh lá': 'Xanh lá',
        'đồng': 'Đồng', 'dong': 'Đồng', 'copper': 'Đồng',
        'be': 'Be', 'beige': 'Be',
        'tím': 'Tím', 'purple': 'Tím',
        'hồng': 'Hồng', 'pink': 'Hồng'
    }
    
    for key, val in colors_map.items():
        if key in s:
            return val
    
    return "Unknown"


def extract_location_smart(text):
    """
    Tìm Tỉnh/Thành phố
    
    Returns:
        str: Tên tỉnh/thành phố, hoặc "Khác"
    """
    if pd.isna(text):
        return "Khác"
    
    s = str(text).lower()
    
    # Danh sách các tỉnh thành phố (mở rộng)
    cities = {
        'hà nội': 'Hà Nội', 'hanoi': 'Hà Nội',
        'hcm': 'TP.HCM', 'hồ chí minh': 'TP.HCM', 'sài gòn': 'TP.HCM', 
        'tp.hcm': 'TP.HCM', 'tp hcm': 'TP.HCM', 'ho chi minh': 'TP.HCM',
        'đà nẵng': 'Đà Nẵng', 'danang': 'Đà Nẵng',
        'hải phòng': 'Hải Phòng', 'haiphong': 'Hải Phòng',
        'cần thơ': 'Cần Thơ', 'cantho': 'Cần Thơ',
        'nghệ an': 'Nghệ An', 'nghe an': 'Nghệ An',
        'bình dương': 'Bình Dương', 'binh duong': 'Bình Dương',
        'đồng nai': 'Đồng Nai', 'dong nai': 'Đồng Nai',
        'hưng yên': 'Hưng Yên', 'hung yen': 'Hưng Yên',
        'bà rịa': 'Bà Rịa - Vũng Tàu', 'vũng tàu': 'Bà Rịa - Vũng Tàu',
        'bà rịa vũng tàu': 'Bà Rịa - Vũng Tàu', 'br-vt': 'Bà Rịa - Vũng Tàu',
        'bắc ninh': 'Bắc Ninh', 'bac ninh': 'Bắc Ninh',
        'hải dương': 'Hải Dương', 'hai duong': 'Hải Dương',
        'thanh hóa': 'Thanh Hóa', 'thanh hoa': 'Thanh Hóa',
        'quảng ninh': 'Quảng Ninh', 'quang ninh': 'Quảng Ninh',
        'khánh hòa': 'Khánh Hòa', 'khanh hoa': 'Khánh Hòa', 'nha trang': 'Khánh Hòa',
        'lâm đồng': 'Lâm Đồng', 'lam dong': 'Lâm Đồng', 'đà lạt': 'Lâm Đồng', 'dalat': 'Lâm Đồng',
        'bình thuận': 'Bình Thuận', 'binh thuan': 'Bình Thuận',
        'kiên giang': 'Kiên Giang', 'kien giang': 'Kiên Giang',
        'thái nguyên': 'Thái Nguyên', 'thai nguyen': 'Thái Nguyên',
        'an giang': 'An Giang',
        'long an': 'Long An',
        'tiền giang': 'Tiền Giang', 'tien giang': 'Tiền Giang',
        'bến tre': 'Bến Tre', 'ben tre': 'Bến Tre',
        'vĩnh long': 'Vĩnh Long', 'vinh long': 'Vĩnh Long',
        'cà mau': 'Cà Mau', 'ca mau': 'Cà Mau'
    }
    
    # Tìm trong text
    for key, val in cities.items():
        if key in s:
            return val
    
    return "Khác"


# ==============================================================================
# 2. PIPELINE CHÍNH
# ==============================================================================

def run_cleaning():
    """
    Pipeline chính để làm sạch dữ liệu từ data/raw
    Đọc file raw mới nhất, extract các trường theo schema, và lưu vào data/cleaned
    """
    RAW_FOLDER = 'data/raw'
    CLEANED_FOLDER = 'data/cleaned'
    os.makedirs(CLEANED_FOLDER, exist_ok=True)
    
    # Tìm file raw mới nhất
    files = glob.glob(os.path.join(RAW_FOLDER, "*.csv"))
    if not files:
        print("⚠️ Không tìm thấy file dữ liệu raw nào!")
        return
    
    # Lấy file mới nhất
    latest_file = max(files, key=os.path.getctime)
    print(f"🔄 Đang xử lý file: {os.path.basename(latest_file)}")
    
    try:
        # Đọc file (Ưu tiên utf-8-sig)
        try:
            df = pd.read_csv(latest_file, encoding='utf-8-sig')
        except:
            print("   ⚠️ Encoding mặc định lỗi, thử UTF-8...")
            df = pd.read_csv(latest_file, encoding='utf-8')
        
        print(f"   -> Tổng số dòng raw: {len(df):,}")
        print(f"   -> Các cột có sẵn: {list(df.columns)}")
        
        # Chuẩn hóa tên cột (nếu cần)
        column_mapping = {
            'gia_xe': 'price_raw',
            'tieu_de': 'title',
            'thong_tin': 'info_raw',
            'gia': 'price_raw'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Đảm bảo có các cột cần thiết
        required_raw_cols = ['title', 'price_raw', 'info_raw']
        missing_cols = [col for col in required_raw_cols if col not in df.columns]
        if missing_cols:
            print(f"   ❌ Thiếu các cột: {missing_cols}")
            return
        
        # Gộp text để tìm kiếm tốt hơn
        df['full_text'] = df['title'].fillna('') + ' | ' + df['info_raw'].fillna('')
        
        print("\n   🔍 Đang trích xuất dữ liệu...")
        
        # --- TRÍCH XUẤT DỮ LIỆU THEO SCHEMA ---
        
        # 1. Price (bắt buộc)
        print("      -> Trích xuất giá...")
        df['price'] = df['price_raw'].apply(clean_price)
        
        # 2. Year (bắt buộc)
        print("      -> Trích xuất năm sản xuất...")
        df['year'] = df.apply(extract_year_smart, axis=1)
        
        # 3. Brand & Model
        print("      -> Trích xuất hãng và dòng xe...")
        df[['brand', 'model']] = df['title'].apply(
            lambda x: pd.Series(extract_brand_model_smart(x))
        )
        
        # 4. Mileage
        print("      -> Trích xuất số km...")
        df['mileage'] = df['full_text'].apply(extract_mileage_smart)
        
        # 5. Fuel
        print("      -> Trích xuất loại nhiên liệu...")
        df['fuel'] = df['full_text'].apply(extract_fuel)
        
        # 6. Location
        print("      -> Trích xuất địa điểm...")
        df['location'] = df['full_text'].apply(extract_location_smart)
        
        # 7. Color
        print("      -> Trích xuất màu sắc...")
        df['color'] = df['full_text'].apply(extract_color)
        
        # 8. Source & Crawl_date (giữ nguyên từ raw data)
        if 'source' not in df.columns:
            df['source'] = df.get('source', 'bonbanh')
        if 'crawl_date' not in df.columns:
            df['crawl_date'] = df.get('crawl_date', datetime.now().strftime("%Y-%m-%d"))
        
        # --- LỌC DỮ LIỆU ---
        print("\n   🧹 Đang lọc dữ liệu...")
        
        # Lọc các dòng không có price hoặc year (bắt buộc)
        before_filter = len(df)
        df_clean = df.dropna(subset=['price', 'year'])
        after_filter = len(df_clean)
        print(f"      -> Đã loại bỏ {before_filter - after_filter} dòng thiếu price/year")
        
        # Lọc theo logic nghiệp vụ
        # - Giá > 50 triệu (tránh xe đồ chơi, phụ tùng)
        # - Năm > 1990 (xe quá cũ)
        # - Năm <= năm hiện tại + 1 (xe tương lai không hợp lý)
        current_year = datetime.now().year
        df_clean = df_clean[
            (df_clean['price'] > 50_000_000) & 
            (df_clean['year'] > 1990) & 
            (df_clean['year'] <= current_year + 1)
        ]
        after_business_filter = len(df_clean)
        print(f"      -> Đã loại bỏ {after_filter - after_business_filter} dòng không hợp lệ")
        
        # --- CHUẨN HÓA KIỂU DỮ LIỆU ---
        print("\n   🔧 Đang chuẩn hóa kiểu dữ liệu...")
        
        # Đảm bảo price, year, mileage là int
        df_clean['price'] = df_clean['price'].astype(int)
        df_clean['year'] = df_clean['year'].astype(int)
        df_clean['mileage'] = df_clean['mileage'].astype(int)
        
        # Chọn các cột theo schema
        schema_cols = ['brand', 'model', 'year', 'price', 'mileage', 'fuel', 
                      'location', 'color', 'source', 'crawl_date']
        
        df_final = df_clean[schema_cols].copy()
        
        # --- LƯU FILE ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        raw_filename = os.path.basename(latest_file)
        source_name = raw_filename.split('_')[0] if '_' in raw_filename else 'bonbanh'
        
        output_filename = f"{source_name}_cleaned_{timestamp}.csv"
        output_path = os.path.join(CLEANED_FOLDER, output_filename)
        
        df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # --- THỐNG KÊ ---
        print("\n" + "="*60)
        print(f"✅ HOÀN TẤT!")
        print(f"📊 Số lượng ban đầu: {before_filter:,} dòng")
        print(f"📊 Số lượng sau khi làm sạch: {len(df_final):,} dòng")
        print(f"📁 File kết quả: {output_filename}")
        print("="*60)
        
        # Hiển thị mẫu dữ liệu
        print("\n--- MẪU DỮ LIỆU SAU KHI LÀM SẠCH ---")
        print(df_final.head(10).to_string())
        
        # Thống kê theo brand
        print("\n--- THỐNG KÊ THEO HÃNG XE (Top 10) ---")
        brand_stats = df_final['brand'].value_counts().head(10)
        print(brand_stats.to_string())
        
    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_cleaning()
