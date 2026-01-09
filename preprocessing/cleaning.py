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

BRAND_MAPPING = {
    'land rover': 'Land Rover', 'range rover': 'Land Rover',
    'mercedes-benz': 'Mercedes-Benz', 'mercedes benz': 'Mercedes-Benz', 
    'mercedes': 'Mercedes-Benz', 'merc': 'Mercedes-Benz', 'mec': 'Mercedes-Benz',
    'bmw': 'BMW', 'audi': 'Audi', 'lexus': 'Lexus', 'porsche': 'Porsche',
    'vinfast': 'VinFast', 'vin fast': 'VinFast',
    'toyota': 'Toyota', 'honda': 'Honda', 'hyundai': 'Hyundai', 'kia': 'Kia',
    'mazda': 'Mazda', 'ford': 'Ford', 'mitsubishi': 'Mitsubishi', 'nissan': 'Nissan',
    'suzuki': 'Suzuki', 'chevrolet': 'Chevrolet', 'peugeot': 'Peugeot',
    'volkswagen': 'Volkswagen', 'vw': 'Volkswagen', 'subaru': 'Subaru',
    'isuzu': 'Isuzu', 'volvo': 'Volvo', 'mini': 'Mini', 'jeep': 'Jeep',
    'mg': 'MG', 'jaguar': 'Jaguar', 'bentley': 'Bentley', 'rolls royce': 'Rolls-Royce',
    'ferrari': 'Ferrari', 'lamborghini': 'Lamborghini', 'maserati': 'Maserati',
    'tesla': 'Tesla', 'byd': 'BYD', 'wuling': 'Wuling'
}

# Các Model xe có tên ghép 2 từ 
MULTI_WORD_MODELS = [
    # --- 1. TOYOTA (Huyền thoại giữ giá) ---
    'corolla altis', 'corolla cross', 'land cruiser', 'land cruiser prado', 
    'fj cruiser', 'urban cruiser', 'yaris cross', 'hilux', 'innova', 
    'fortuner', 'alphard', 'veloz', 'avanza', 'wigo', 'rush', 'raize',
    'hiace', 'previa', 'zace', # Zace là trùm xe cũ

    # --- 2. HYUNDAI (Xe Hàn quốc dân) ---
    'grand i10', 'i10', 'i20', 'i30', 'santa fe', 'tucson', 'accent', 
    'elantra', 'sonata', 'creta', 'venue', 'custin', 'palisade', 'stargazer', 
    'kona', 'getz', 'click', 'starex', 'terracan', 'galloper', 'genesis', 'veloster',

    # --- 3. KIA (Phổ biến) ---
    'morning', 'new morning', 'soluto', 'seltos', 'sonet', 'sorento', 
    'carnival', 'sedona', 'cerato', 'k3', 'k5', 'optima', 'rondo', 'carens', 
    'sportage', 'rio', 'spectra', 'cd5', 'pride', # Xe tập lái huyền thoại

    # --- 4. MAZDA (Dòng CX và BT) ---
    'cx 3', 'cx 30', 'cx 5', 'cx 8', 'cx 9', 'bt 50', 'bt-50', 
    'mazda 2', 'mazda 3', 'mazda 6', 'premacy', # Premacy đời cũ

    # --- 5. FORD (Vua bán tải & SUV) ---
    'ranger', 'ranger raptor', 'everest', 'explorer', 'territory', 'ecosport', 
    'transit', 'tourneo', 'focus', 'fiesta', 'mondeo', 'escape', 'laser', # Laser chạy rất bền

    # --- 6. HONDA ---
    'cr v', 'hr v', 'br v', 'wr v', 'zr v', 'city', 'civic', 'accord', 
    'brio', 'jazz', 'odyssey', 'stream',

    # --- 7. MITSUBISHI ---
    'pajero', 'pajero sport', 'xpander', 'xpander cross', 'outlander', 'outlander sport',
    'triton', 'attrage', 'mirage', 'grandis', 'jolie', 'zinger', 'lancer', 'lancer gala',

    # --- 8. CHEVROLET & DAEWOO (Thị trường xe cỏ giá rẻ) ---
    'cruze', 'aveo', 'spark', 'spark van', 'captiva', 'orlando', 'colorado', 'trailblazer',
    'lacetti', 'lacetti cdx', 'lacetti se', 'gentra', 'gentra x', 
    'matiz', 'lanos', 'nubira', 'magnus', 'leganza', # Daewoo cũ

    # --- 9. SUZUKI ---
    'xl7', 'ertiga', 'swift', 'ciaz', 'jimny', 'vitara', 'grand vitara', 
    'blind van', 'carry', 'wagon',

    # --- 10. NISSAN ---
    'navara', 'terra', 'x trail', 'x-trail', 'almera', 'sunny', 'teana', 'kicks', 
    'tiida', 'grand livina', 'livina', 'bluebird',

    # --- 11. VINFAST ---
    'vf 3', 'vf 5', 'vf 6', 'vf 7', 'vf 8', 'vf 9', 'vf e34', 
    'lux a', 'lux a2.0', 'lux sa', 'lux sa2.0', 'fadil', 'president',

    # --- 12. MERCEDES-BENZ (Dòng Class & G) ---
    'c class', 'e class', 's class', 'a class', 'glc', 'gle', 'gla', 'glb', 'gls', 'glk', 
    'maybach', 'v class', 'g class', 'amg', 'sprinter', # Sprinter xe 16 chỗ

    # --- 13. BMW ---
    '3 series', '5 series', '7 series', 'x1', 'x3', 'x4', 'x5', 'x6', 'x7', 
    '320i', '325i', '520i', '523i', '528i', # Các dòng số phổ biến

    # --- 14. ISUZU ---
    'd max', 'mu x', 'hilander', 'trooper',

    # --- 15. KHÁC (Subaru, Peugeot, MG...) ---
    'forester', 'outback', # Subaru
    '3008', '5008', '2008', '408', 'traveller', # Peugeot
    'zs', 'hs', 'rx5', 'mg5', # MG
    'beijing x7', # Xe Tàu hot
]

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
        # Validate: năm hợp lý từ 1990 đến năm hiện tại + 1
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


def extract_brand_model_smart(title, source='bonbanh'):
    """
    Tách Brand và Model thông minh (Hỗ trợ 3 nguồn: bonbanh, otocomvn, chotot)
    """
    # 1. Kiểm tra dữ liệu rỗng
    if pd.isna(title) or str(title).strip() == "":
        return "Other", "Other"
    
    # 2. Tiền xử lý chung (Xóa năm, đưa về chữ thường)
    raw_title = str(title).lower().strip()
    clean_text = re.sub(r'\b(19|20)\d{2}\b', ' ', raw_title) # Xóa năm 19xx-20xx

    # 3. Xử lý riêng cho từng nguồn (Source-specific logic)
    if source == 'bonbanh':
        # Bonbanh: Thường có "Xe cũ", "Xe mới", và giá sau dấu "-"
        clean_text = clean_text.replace('xe cũ', '').replace('xe mới', '')
        
        # Xóa dấu gạch ngang đầu câu (do xóa năm để lại)
        clean_text = clean_text.strip()
        if clean_text.startswith('-') or clean_text.startswith('–'):
            clean_text = clean_text[1:].strip()
            
        # Cắt bỏ phần giá/địa điểm sau dấu gạch ngang (nếu có)
        if ' - ' in clean_text:
            parts = clean_text.split(' - ')
            # Logic: Nếu phần đầu dài (chứa tên xe) thì lấy, nếu ngắn quá (mã tin) thì lấy phần sau
            if len(parts[0].strip()) > 3: 
                clean_text = parts[0]
            elif len(parts) > 1:
                clean_text = parts[1]

    elif source == 'chotot':
        # Chotot: Nhiều từ rác cảm thán
        stopwords = ['cần bán', 'bán gấp', 'thanh lý', 'giá rẻ', 'xe nhà', 'chính chủ', 'gia đình', 'bán xe']
        for word in stopwords:
            clean_text = clean_text.replace(word, '')
            
    elif source == 'otocomvn':
        # Oto.com.vn: Khá sạch, chỉ cần bỏ chữ "bán xe"
        clean_text = clean_text.replace('bán xe', '')

    # 4. Làm sạch ký tự đặc biệt
    clean_text = re.sub(r'[^\w\s]', ' ', clean_text)
    clean_text = " ".join(clean_text.split())

    # 5. Tìm Hãng xe (Brand)
    found_brand = "Other"
    found_brand_key = ""
    
    for key in BRAND_MAPPING:
        # Dùng regex \b để khớp đúng từ (vd: tránh khớp "mazda" trong "amazda")
        if re.search(r'\b' + re.escape(key) + r'\b', clean_text):
            found_brand = BRAND_MAPPING[key]
            found_brand_key = key
            break 
    
    if found_brand == "Other":
        return "Other", "Other"

    # 6. Tìm Dòng xe (Model)
    # Xóa tên hãng đã tìm được khỏi chuỗi để tìm model trong phần còn lại
    model_part = re.sub(r'\b' + re.escape(found_brand_key) + r'\b', '', clean_text).strip()
    found_model = "Other"
    model_tokens = model_part.split()
    
    if model_tokens:
        # Ưu tiên tìm model ghép 2 từ (VD: Land Cruiser, CX 5)
        if len(model_tokens) >= 2:
            two_words = model_tokens[0] + " " + model_tokens[1]
            two_words_norm = two_words.replace('-', ' ') # Chuẩn hóa gạch ngang
            
            if two_words_norm in MULTI_WORD_MODELS:
                found_model = two_words_norm
            # Logic đặc biệt cho Mercedes S Class, C Class...
            elif found_brand == 'Mercedes-Benz' and model_tokens[1] == 'class':
                found_model = model_tokens[0] + " Class"
            else:
                found_model = model_tokens[0] # Lấy từ đầu tiên
        else:
            found_model = model_tokens[0] # Lấy từ duy nhất còn lại

    # 7. Chuẩn hóa tên Model lần cuối (Post-processing)
    found_model = found_model.upper()
    
    if found_model.startswith('VF'): # VF3 -> VF 3
        found_model = found_model.replace('VF', 'VF ').replace('  ', ' ').strip()
    if found_model.startswith('CX'): # CX5 -> CX 5
        found_model = found_model.replace('CX', 'CX ').replace('  ', ' ').strip()
    if 'CLASS' in found_model: # SCLASS -> S CLASS
        found_model = found_model.replace('CLASS', ' CLASS').replace('  ', ' ').strip()
        
    return found_brand, found_model.title()


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
        
        # Chuẩn hóa tên cột
        column_mapping = {
            'gia_xe': 'price_raw',
            'tieu_de': 'title',
            'thong_tin': 'info_raw',
            'gia': 'price_raw',
            'link': 'url'
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

        # Đảm bảo cột url tồn tại
        if 'url' not in df_clean.columns:
            # Nếu không có cột url, thử tìm xem trong df gốc có không
            if 'url' in df.columns:
                df_clean['url'] = df['url']
            else:
                # Nếu vẫn không có, tạo cột rỗng (để không bị lỗi)
                df_clean['url'] = ""

    
        # Chọn các cột theo schema
        schema_cols = ['brand', 'model', 'year', 'price', 'mileage', 'fuel', 
                      'location', 'color', 'source', 'crawl_date', 'url']
        available_cols = [c for c in schema_cols if c in df_clean.columns]
        df_final = df_clean[available_cols].copy()
        
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
