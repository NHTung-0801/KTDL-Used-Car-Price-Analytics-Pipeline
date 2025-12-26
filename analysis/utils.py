# analysis/utils.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os
import json
from . import config  # Import file config cùng thư mục

def setup_style():
    """Thiết lập giao diện chung cho Matplotlib/Seaborn"""
    sns.set_theme(style="whitegrid", context="notebook", font_scale=config.FONT_SCALE)
    plt.rcParams['figure.figsize'] = config.FIG_SIZE
    plt.rcParams['savefig.dpi'] = config.DPI
    # Hỗ trợ font tiếng Việt (nếu máy có font)
    # plt.rcParams['font.family'] = 'sans-serif' 

def load_master_data():
    """
    Tự động tìm file csv mới nhất trong thư mục data/master
    """
    pattern = os.path.join(config.DATA_DIR, 'master_dataset_all_*.csv')
    files = glob.glob(pattern)
    
    if not files:
        print(f"❌ [UTILS] Không tìm thấy file dữ liệu nào tại: {config.DATA_DIR}")
        print("   -> Hãy chạy 'python preprocessing/transformation.py' trước.")
        return None
        
    # Lấy file mới nhất theo thời gian tạo
    latest_file = max(files, key=os.path.getctime)
    print(f"📂 [UTILS] Đang nạp dữ liệu: {os.path.basename(latest_file)}")
    
    try:
        df = pd.read_csv(latest_file)
        return df
    except Exception as e:
        print(f"❌ [UTILS] Lỗi đọc file: {e}")
        return None

def save_static_plot(fig_obj, filename):
    """
    Lưu biểu đồ tĩnh (Matplotlib/Seaborn) vào thư mục output
    Usage: 
       save_static_plot(plt, 'pair_a_price.png')
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, filename)
    
    # fig_obj thường là plt hoặc figure
    fig_obj.savefig(path, bbox_inches='tight')
    print(f"   📸 [SAVED] Ảnh báo cáo: {filename}")

def save_interactive_plot(fig, filename):
    """
    Lưu biểu đồ động (Plotly) vào thư mục output dưới dạng JSON
    Usage:
       save_interactive_plot(fig, 'pair_a_scatter.json')
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, filename)
    
    # Lưu JSON để Web Streamlit đọc
    fig.write_json(path)
    print(f"   💾 [SAVED] Web Data: {filename}")