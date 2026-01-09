import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
# Import các công cụ dùng chung
from analysis import utils, config

def run_analysis(df):
    print("\n--- [CẶP B] Đang xử lý... ---")
    
    # ==============================================================================
    # 1. Biểu đồ Treemap - Cấu trúc thị trường (Plotly)
    # ==============================================================================
    print("   -> Đang tạo Treemap - Cấu trúc thị trường...")
    
    # Tính toán số lượng xe theo brand và model
    market_structure = df.groupby(['brand', 'model']).size().reset_index(name='count')
    
    # Lọc bớt các model quá ít xe để biểu đồ đỡ rối (giữ lại model > 5 xe)
    market_structure = market_structure[market_structure['count'] > 5]

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
        textfont_size=14
    )
    
    fig_treemap.update_layout(
        margin=dict(t=50, l=10, r=10, b=10),
        height=600
    )
    
    # --- SỬA TÊN FILE TẠI ĐÂY (pair_a -> pair_b) ---
    utils.save_interactive_plot(fig_treemap, "pair_b_treemap_market.json")
    
    # ==============================================================================
    # 2. Biểu đồ Bar Chart - Phân bố Vùng miền (Seaborn)
    # ==============================================================================
    print("   -> Đang tạo Bar Chart - Phân bố Vùng miền...")
    
    # Kiểm tra cột location
    region_col = 'location' if 'location' in df.columns else 'source'
    
    # Tính số lượng xe theo vùng miền và chỉ lấy TOP 20 để biểu đồ không bị quá dài
    region_counts = df[region_col].value_counts().head(20).sort_values(ascending=True)
    
    if len(region_counts) == 0:
        print("      ⚠️ Không có dữ liệu vùng miền để vẽ.")
    else:
        # Vẽ biểu đồ cột ngang
        plt.figure(figsize=(12, 8)) # Tăng kích thước để dễ nhìn
        
        # Dùng palette màu khác cho đẹp (Spectra)
        colors = sns.color_palette("Spectral", len(region_counts))
        
        bars = plt.barh(range(len(region_counts)), region_counts.values, color=colors)
        plt.yticks(range(len(region_counts)), region_counts.index, fontsize=11)
        plt.xlabel('Số lượng xe', fontsize=12)
        plt.title(f'Top 20 Khu vực phân bố xe ({region_col})', fontsize=14, fontweight='bold')
        
        # Thêm giá trị lên mỗi cột
        for i, (bar, value) in enumerate(zip(bars, region_counts.values)):
            plt.text(value + (max(region_counts.values)*0.01), i, f' {value:,}', 
                    va='center', fontsize=10, fontweight='bold', color='#333333')
        
        plt.grid(axis='x', alpha=0.3, linestyle='--')
        plt.tight_layout()
        
        # --- SỬA TÊN FILE TẠI ĐÂY (pair_a -> pair_b) ---
        utils.save_static_plot(plt, "pair_b_bar_region.png")
        plt.close()
    
    print("✅ Hoàn thành Cặp B!")
    print(f"   - Treemap lưu tại: pair_b_treemap_market.json")
    print(f"   - BarChart lưu tại: pair_b_bar_region.png")