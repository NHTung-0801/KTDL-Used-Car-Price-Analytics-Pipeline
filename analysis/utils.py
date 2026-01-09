import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os
import platform

# Import config từ cùng thư mục
try:
    from . import config
except ImportError:
    import config

def setup_style():
    """
    Thiết lập giao diện chung cho Matplotlib/Seaborn.
    Tự động chọn font hỗ trợ tiếng Việt dựa trên hệ điều hành.
    """
    # 1. Setup Theme
    sns.set_theme(style="whitegrid", context="notebook", font_scale=config.FONT_SCALE)
    plt.rcParams['figure.figsize'] = config.FIG_SIZE
    plt.rcParams['savefig.dpi'] = config.DPI
    
    # 2. Setup Font tiếng Việt (Tránh lỗi ô vuông)
    system = platform.system()
    if system == 'Windows':
        # Segoe UI hoặc Arial thường có sẵn trên Windows và hỗ trợ tiếng Việt tốt
        plt.rcParams['font.family'] = 'Segoe UI'
    elif system == 'Darwin': # macOS
        plt.rcParams['font.family'] = 'AppleGothic'
    elif system == 'Linux':
        plt.rcParams['font.family'] = 'DejaVu Sans' # Font mặc định an toàn trên Linux

    print(f"🎨 [STYLE] Đã thiết lập giao diện (OS: {system})")

def load_master_data():
    """
    Tự động tìm file csv mới nhất trong thư mục data/master
    """
    # Tên file khớp với output của cleaning.py: master_dataset_final_...
    pattern = os.path.join(config.DATA_DIR, 'master_dataset_final_*.csv')
    
    files = glob.glob(pattern)
    
    if not files:
        print(f"❌ [UTILS] Không tìm thấy file dữ liệu tại: {config.DATA_DIR}")
        print(f"   -> Đang tìm file mẫu: 'master_dataset_final_*.csv'")
        print("   -> Hãy chạy 'python preprocessing/transformation.py' (hoặc cleaning) trước.")
        return None
        
    # Lấy file mới nhất theo thời gian tạo
    latest_file = max(files, key=os.path.getctime)
    print(f"📂 [UTILS] Đang nạp dữ liệu: {os.path.basename(latest_file)}")
    
    try:
        df = pd.read_csv(latest_file)
        # Ép kiểu lại crawl_date sang datetime để vẽ biểu đồ thời gian nếu cần
        if 'crawl_date' in df.columns:
            df['crawl_date'] = pd.to_datetime(df['crawl_date'])
        return df
    except Exception as e:
        print(f"❌ [UTILS] Lỗi đọc file: {e}")
        return None

def save_static_plot(fig_obj, filename):
    """
    Lưu biểu đồ tĩnh (Matplotlib/Seaborn)
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, filename)
    
    # fig_obj có thể là plt hoặc figure object
    fig_obj.savefig(path, bbox_inches='tight')
    print(f"   📸 [SAVED] Ảnh: {filename}")

def save_interactive_plot(fig, filename):
    """
    Lưu biểu đồ động (Plotly) dưới dạng JSON
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, filename)
    
    # Lưu JSON để Web Streamlit đọc
    fig.write_json(path)
    print(f"   💾 [SAVED] JSON: {filename}")