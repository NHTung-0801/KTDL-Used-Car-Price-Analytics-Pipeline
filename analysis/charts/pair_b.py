import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
# Import các công cụ dùng chung
from analysis import utils, config

def run_analysis(df):
    print("\n--- [CẶP B] Đang xử lý... ---")
    
    # 1. Biểu đồ Treemap - Cấu trúc thị trường (Plotly)
    print("Đang tạo Treemap - Cấu trúc thị trường...")
    
    # Tính toán số lượng xe theo brand và model
    market_structure = df.groupby(['brand', 'model']).size().reset_index(name='count')
    
    fig_treemap = px.treemap(
        market_structure,
        path=['brand', 'model'],  # Phân cấp: brand -> model
        values='count',
        title='Cấu trúc Thị trường - Phân bố Thương hiệu và Model',
        color='count',
        color_continuous_scale='Blues',
        hover_data={'count': ':,'}
    )
    
    fig_treemap.update_traces(
        textinfo="label+value",
        textfont_size=12
    )
    
    fig_treemap.update_layout(
        margin=dict(t=50, l=10, r=10, b=10),
        height=600
    )
    
    # Lưu biểu đồ động
    utils.save_interactive_plot(fig_treemap, "pair_a_treemap_market.json")
    
    # 2. Biểu đồ Bar Chart - Phân bố Vùng miền (Seaborn)
    print("Đang tạo Bar Chart - Phân bố Vùng miền...")
    
    # Kiểm tra xem có cột 'region' không, nếu không thì dùng cột khác
    if 'region' in df.columns:
        region_col = 'region'
    elif 'city' in df.columns:
        region_col = 'city'
    elif 'location' in df.columns:
        region_col = 'location'
    else:
        print("Không tìm thấy cột vùng miền, sử dụng brand làm ví dụ")
        region_col = 'brand'
    
    # Tính số lượng xe theo vùng miền
    region_counts = df[region_col].value_counts().sort_values(ascending=True)
    
    # Vẽ biểu đồ cột ngang
    plt.figure(figsize=(10, max(6, len(region_counts) * 0.3)))
    colors = sns.color_palette("viridis", len(region_counts))
    
    bars = plt.barh(range(len(region_counts)), region_counts.values, color=colors)
    plt.yticks(range(len(region_counts)), region_counts.index)
    plt.xlabel('Số lượng xe', fontsize=12)
    plt.ylabel('Vùng miền / Khu vực', fontsize=12)
    plt.title('Phân bố Xe theo Vùng miền', fontsize=14, fontweight='bold')
    
    # Thêm giá trị lên mỗi cột
    for i, (bar, value) in enumerate(zip(bars, region_counts.values)):
        plt.text(value, i, f' {value:,}', 
                va='center', fontsize=9)
    
    plt.grid(axis='x', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Lưu biểu đồ tĩnh
    utils.save_static_plot(plt, "pair_a_bar_region.png")
    plt.close()
    
    print("Hoàn thành Cặp A!")
    print(f"  - Treemap: Phân tích {len(market_structure)} tổ hợp brand-model")
    print(f"  - Bar Chart: Phân tích {len(region_counts)} vùng miền")