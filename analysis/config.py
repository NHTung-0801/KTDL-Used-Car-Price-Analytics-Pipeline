# analysis/config.py
import os

# --- 1. CẤU HÌNH ĐƯỜNG DẪN ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'master')
OUTPUT_DIR = os.path.join(BASE_DIR, 'analysis', 'output')

# --- 2. CẤU HÌNH MÀU SẮC (Dùng cho đẹp và đồng bộ) ---
# Bảng màu chuẩn cho các Hãng xe (để biểu đồ nào cũng giống nhau)
BRAND_COLORS = {
    'Toyota': '#EB3324',   # Đỏ
    'Hyundai': '#002C5F',  # Xanh đậm
    'Kia': '#BB162B',      # Đỏ đậm
    'Mazda': '#101010',    # Đen/Xám
    'Ford': '#2C3E50',     # Xanh
    'Mercedes': '#CCCCCC', # Bạc
    'BMW': '#0066B1',      # Xanh BMW
    'Vinfast': '#808080',  # Xám
    'Other': '#95a5a6'     # Xám nhạt
}

# --- 3. CẤU HÌNH BIỂU ĐỒ ---
FIG_SIZE = (12, 6)
DPI = 300 # Độ phân giải ảnh (300 là chuẩn in ấn/báo cáo)
FONT_SCALE = 1.1