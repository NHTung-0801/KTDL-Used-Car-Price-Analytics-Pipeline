import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

# Import các công cụ dùng chung
from analysis import utils, config

def run_analysis(df):
    print("\n--- [CẶP A] Đang xử lý... ---")
    
    # 1. Ví dụ vẽ biểu đồ tĩnh (Seaborn)
    plt.figure() # Khởi tạo hình
    sns.histplot(df['price'])
    plt.title("Ví dụ Phân phối giá")
    
    # Lưu bằng hàm dùng chung
    utils.save_static_plot(plt, "pair_a_example.png")
    plt.close()

    # 2. Ví dụ vẽ biểu đồ động (Plotly)
    fig = px.scatter(df, x='year', y='price', 
                     color='brand', 
                     color_discrete_map=config.BRAND_COLORS) # Dùng màu chuẩn
    
    # Lưu bằng hàm dùng chung
    utils.save_interactive_plot(fig, "pair_a_example.json")