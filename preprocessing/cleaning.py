import pandas as pd
import glob
import os
import re
from datetime import datetime
# ==============================================================================
# 1. CÁC HÀM TRÍCH XUẤT (EXTRACTORS)
# ==============================================================================

def clean_price(price_raw):
    """Xử lý giá tiền (Hỗ trợ 'tỉ' và 'tỷ')"""
    if pd.isna(price_raw): return None
    p_str = str(price_raw).lower().strip().replace('tỉ', 'tỷ') # Fix lỗi chính tả phổ biến
    
    if 'liên hệ' in p_str or 'thỏa thuận' in p_str: return None
    
    p_str = p_str.replace('.', '').replace(',', '')
    
    # Xử lý Tỷ + Triệu
    if 'tỷ' in p_str:
        try:
            parts = p_str.split('tỷ')
            ty = int(re.findall(r'\d+', parts[0])[0])
            trieu = 0
            if len(parts) > 1 and 'triệu' in parts[1]:
                tr = re.findall(r'\d+', parts[1])
                trieu = int(tr[0]) if tr else 0
            return int(ty * 1_000_000_000 + trieu * 1_000_000)
        except: return None
    # Xử lý Triệu
    elif 'triệu' in p_str:
        try:
            return int(re.findall(r'\d+', p_str)[0]) * 1_000_000
        except: 
            return None
    # Số thuần
    else:
        try:
            nums = re.findall(r'\d+', p_str)
            return int(max(nums, key=len)) if nums else None
        except: 
            return None

def extract_year_smart(row):
    """Tìm năm sản xuất: Ưu tiên Title, sau đó đến Info"""
    # 1. Title thường có dạng: "2015 - Kia Rio..."
    title = str(row.get('title', ''))
    match = re.search(r'\b(19|20)\d{2}\b', title)
    if match: return int(match.group(0))
    
    # 2. Nếu không có, tìm trong chuỗi info dài ngoằng
    info = str(row.get('info_raw', ''))
    match = re.search(r'\b(19|20)\d{2}\b', info)
    return int(match.group(0)) if match else None

def extract_fuel(text):
    """Quét từ khóa nhiên liệu"""
    if pd.isna(text): return "Unknown"
    s = str(text).lower()
    
    if 'điện' in s or 'ev ' in s: return 'Điện'
    if 'hybrid' in s: return 'Hybrid'
    if 'dầu' in s or 'diesel' in s: return 'Dầu'
    if 'xăng' in s: return 'Xăng'
    return "Unknown"

def extract_color(text):
    """Tìm màu sắc trong văn bản"""
    if pd.isna(text): return "Unknown"
    s = str(text).lower()
    
    colors_map = {
        'trắng': 'Trắng', 'đen': 'Đen', 'đỏ': 'Đỏ', 'bạc': 'Bạc', 
        'xám': 'Xám', 'ghi': 'Xám', 'nâu': 'Nâu', 'vàng': 'Vàng', 
        'cam': 'Cam', 'xanh': 'Xanh', 'đồng': 'Đồng', 'be': 'Be'
    }
    
    for key, val in colors_map.items():
        if key in s: return val
    return "Unknown"

def extract_mileage_smart(text):
    """Tìm số Km (ODO)"""
    if pd.isna(text): return 0
    s = str(text).lower().replace('.', '').replace(',', '')
    # Tìm số đứng ngay trước chữ km
    match = re.search(r'(\d+)\s*km', s)
    return int(match.group(1)) if match else 0

def extract_location_smart(text):
    """Tìm Tỉnh/Thành phố trong chuỗi thông tin"""
    if pd.isna(text): return "Khác"
    s = str(text).lower()
    
    # Danh sách các tỉnh thành phố lớn (có thể bổ sung thêm)
    cities = {
        'hà nội': 'Hà Nội', 'hcm': 'TP.HCM', 'hồ chí minh': 'TP.HCM', 'sài gòn': 'TP.HCM', 
        'đà nẵng': 'Đà Nẵng', 'hải phòng': 'Hải Phòng', 'cần thơ': 'Cần Thơ', 
        'nghệ an': 'Nghệ An', 'bình dương': 'Bình Dương', 'đồng nai': 'Đồng Nai',
        'hưng yên': 'Hưng Yên', 'bà rịa': 'Bà Rịa', 'vũng tàu': 'Vũng Tàu',
        'bắc ninh': 'Bắc Ninh', 'hải dương': 'Hải Dương', 'thanh hóa': 'Thanh Hóa',
        'quảng ninh': 'Quảng Ninh', 'khánh hòa': 'Khánh Hòa', 'nha trang': 'Khánh Hòa',
        'lâm đồng': 'Lâm Đồng', 'đà lạt': 'Lâm Đồng', 'bình thuận': 'Bình Thuận',
        'kiên giang': 'Kiên Giang', 'thái nguyên': 'Thái Nguyên'
    }
    
    # Quét ngược từ cuối chuỗi lên (vì địa chỉ thường nằm cuối tin đăng)
    for key, val in cities.items():
        if key in s: return val
    return "Khác"

def extract_brand_model_smart(title):
    """Tách Brand và Model từ tiêu đề (Ví dụ: 2015 - Kia Rio...)"""
    if pd.isna(title): return "Other", "Other"
    
    # Bỏ năm và dấu gạch ngang ở đầu: "2015 - "
    clean_title = re.sub(r'^(19|20)\d{2}\s*[-–]\s*', '', str(title).strip())
    
    parts = clean_title.split()
    
    # Danh sách hãng xe phổ biến
    brands_list = [
        'toyota', 'hyundai', 'kia', 'mazda', 'honda', 'ford', 'mercedes', 'bmw', 
        'audi', 'vinfast', 'mitsubishi', 'nissan', 'suzuki', 'lexus', 'porsche', 
        'land rover', 'mg', 'peugeot', 'volvo', 'subaru', 'isuzu'
    ]
    
    brand, model = "Other", "Other"
    
    if len(parts) > 0:
        # Xác định vị trí bắt đầu (bỏ qua từ 'Xe', 'Bán' nếu có)
        start_idx = 0
        if parts[0].lower() in ['xe', 'bán', 'cần']: start_idx = 1
        
        if start_idx < len(parts):
            first_word = parts[start_idx].lower()
            
            # Kiểm tra xem từ đầu tiên có phải là Hãng không
            found_brand = False
            for b in brands_list:
                if b in first_word: # match 'mercedes-benz' với 'mercedes'
                    brand = b.title() # Viết hoa chữ cái đầu (Toyota)
                    if b == 'bmw' or b == 'mg': brand = b.upper() # Viết hoa hết (BMW)
                    found_brand = True
                    break
            
            if not found_brand:
                # Nếu không tìm thấy trong list, cứ lấy từ đầu tiên làm Brand (Heuristic)
                brand = parts[start_idx].title()
            
            # Lấy Model (thường là từ ngay sau Brand)
            if len(parts) > start_idx + 1:
                model = parts[start_idx + 1]
                
    return brand, model

# ==============================================================================
# 2. PIPELINE CHÍNH
# ==============================================================================

def run_cleaning():
    RAW_FOLDER = 'data/raw'
    CLEANED_FOLDER = 'data/cleaned'
    os.makedirs(CLEANED_FOLDER, exist_ok=True)
    
    files = glob.glob(os.path.join(RAW_FOLDER, "*.csv"))
    if not files: return print("⚠️ Không tìm thấy file dữ liệu raw nào!")
    
    # Lấy file mới nhất
    latest_file = max(files, key=os.path.getctime)
    print(f"🔄 Đang xử lý file: {os.path.basename(latest_file)}")
    
    try:
        # Đọc file (Ưu tiên utf-8-sig)
        try:
            df = pd.read_csv(latest_file, encoding='utf-8-sig')
        except:
            print("   ⚠️ Encoding mặc định lỗi, thử UTF-16...")
            df = pd.read_csv(latest_file, sep='\t', encoding='utf-16')

        # Chuẩn hóa tên cột
        df.rename(columns={'gia_xe': 'price_raw', 'tieu_de': 'title', 'thong_tin': 'info_raw'}, inplace=True)
        
        # --- BƯỚC QUAN TRỌNG: GỘP TEXT ---
        # File của bạn info_raw đã rất đầy đủ, nhưng ta gộp thêm title vào để chắc chắn 
        # không sót từ khóa nào (đặc biệt là Năm và Model xe)
        df['full_text'] = df['title'].fillna('') + ' | ' + df['info_raw'].fillna('')
        
        print(f"   -> Tổng số dòng raw: {len(df)}")

        # --- TRÍCH XUẤT DỮ LIỆU ---
        
        # 1. Giá & Năm (Bắt buộc phải có)
        df['price'] = df['price_raw'].apply(clean_price)
        df['year'] = df.apply(extract_year_smart, axis=1)
        
        # 2. Các thông số khác (Tìm trong full_text)
        df['mileage'] = df['full_text'].apply(extract_mileage_smart)
        df['fuel'] = df['full_text'].apply(extract_fuel)
        df['color'] = df['full_text'].apply(extract_color)
        df['location'] = df['full_text'].apply(extract_location_smart)
        
        # 3. Brand & Model (Phân tích Title)
        df[['brand', 'model']] = df['title'].apply(lambda x: pd.Series(extract_brand_model_smart(x)))
        
        # 4. Điền các cột còn thiếu
        req_cols = ['brand', 'model', 'year', 'price', 'mileage', 'fuel', 'location', 'color', 'source', 'crawl_date', 'url']
        for col in req_cols:
            if col not in df.columns: df[col] = None if col != 'mileage' else 0

        # --- LỌC & LÀM SẠCH ---
        # Xóa các dòng không lấy được Giá hoặc Năm (Dữ liệu rác)
        df_clean = df.dropna(subset=['price', 'year'])
        
        # Logic lọc: Giá > 50 triệu VÀ Năm > 1990 (Tránh xe đồ chơi hoặc xe quá nát)
        df_clean = df_clean[(df_clean['price'] > 50_000_000) & (df_clean['year'] > 1990)]
        
        # Chọn đúng cột cần dùng
        df_final = df_clean[req_cols]
        
        # --- [PHẦN QUAN TRỌNG] TẠO TÊN FILE CÓ TIMESTAMP VÀ NGUỒN ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        
        # 1. Lấy tên file gốc để xác định nguồn (Ví dụ: bonbanh_full_... -> bonbanh)
        # (Biến latest_file đã có ở đầu hàm run_cleaning)
        raw_filename = os.path.basename(latest_file)
        source_name = raw_filename.split('_')[0] 
        
        # 2. Đặt tên file output: {tên_nguồn}_cleaned_{ngày_giờ}.csv
        output_filename = f"{source_name}_cleaned_{timestamp}.csv"
        output_path = os.path.join(CLEANED_FOLDER, output_filename)
        
        df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ HOÀN TẤT! Đã lọc được: {len(df_final)} xe sạch.")
        print(f"   -> Đã loại bỏ: {len(df) - len(df_final)} dòng rác/lỗi.")
        print(f"   -> File kết quả: {output_path}")
        
        # In thử vài dòng để bạn kiểm tra
        print("\n--- MẪU DỮ LIỆU SAU KHI LỌC ---")
        print(df_final[['brand', 'model', 'year', 'price', 'fuel', 'location', 'color']].head(10))

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")

if __name__ == "__main__":
    run_cleaning()