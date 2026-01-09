import os

# --- 1. CẤU HÌNH ĐƯỜNG DẪN ---
# Lùi 2 cấp từ file config.py (analysis/config.py) để về root project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'master')
# Output nằm trong analysis/output là hợp lý
OUTPUT_DIR = os.path.join(BASE_DIR, 'analysis', 'output') 

# Tạo thư mục output nếu chưa có
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 2. CẤU HÌNH MÀU SẮC (ĐÃ SỬA LẠI KEY CHO KHỚP DATA) ---
# Màu sắc mang tính nhận diện thương hiệu
BRAND_COLORS = {
    'Toyota': '#EB3324',       # Đỏ Toyota
    'Hyundai': '#002C5F',      # Xanh Navy
    'Kia': '#BB162B',          # Đỏ Kia
    'Mazda': '#101010',        # Đen/Xám Mazda
    'Ford': '#2C3E50',         # Xanh Ford
    'Mercedes-Benz': '#8E99A5',# Bạc (Sửa lại key đúng với cleaning)
    'BMW': '#0066B1',          # Xanh BMW
    'VinFast': '#000000',      # Đen quyền lực (Sửa lại VinFast chữ F hoa)
    'Honda': '#CC0000',        # Đỏ Honda
    'Mitsubishi': '#E60012',   # Đỏ Mít
    'Land Rover': '#005A2B',   # Xanh lá cây đặc trưng
    'Other': '#95a5a6'         # Xám nhạt cho các hãng khác
}

# Hàm an toàn để lấy màu (tránh lỗi KeyError nếu gặp hãng lạ)
def get_brand_color(brand_name):
    return BRAND_COLORS.get(brand_name, BRAND_COLORS['Other'])

# --- 3. CẤU HÌNH BIỂU ĐỒ ---
FIG_SIZE = (12, 6)
DPI = 300 
FONT_SCALE = 1.1

# Cấu hình Font chữ để hiển thị Tiếng Việt không bị lỗi ô vuông
# Nếu bạn chạy trên Windows, thường font mặc định là Arial hoặc Segoe UI
import matplotlib.pyplot as plt
import seaborn as sns
import platform

def set_vietnamese_font():
    system = platform.system()
    if system == 'Windows':
        plt.rcParams['font.family'] = 'Segoe UI' # Hoặc 'Arial'
    elif system == 'Darwin': # MacOS
        plt.rcParams['font.family'] = 'AppleGothic'
    else: # Linux/Colab
        plt.rcParams['font.family'] = 'DejaVu Sans' # Có thể cần cài thêm font

# Gọi hàm set font ngay khi import config (hoặc gọi thủ công)
# set_vietnamese_font()