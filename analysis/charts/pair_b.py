import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import pandas as pd
import numpy as np

# Import module nội bộ
from analysis import utils, config

def prepare_data_market(df):
    """
    Chuẩn bị dữ liệu cho Phân tích Thị trường (Pair B)
    """
    df_clean = df.copy()
    
    # Chuẩn hóa tên Hãng và Model
    df_clean['brand'] = df_clean['brand'].fillna('Unknown').astype(str).str.title()
    df_clean['model'] = df_clean['model'].fillna('Unknown').astype(str).str.title()
    
    # Xử lý cột vị trí (Location)
    if 'location' not in df_clean.columns:
        df_clean['location'] = df_clean['source']
    
    df_clean['location'] = df_clean['location'].fillna('Toàn Quốc').astype(str).str.strip().str.title()
    
    return df_clean

def run_analysis(df_master):
    print("\n--- [CẶP B] PHÂN TÍCH THỊ TRƯỜNG & NGUỒN CUNG ---")
    
    df = prepare_data_market(df_master)
    print(f"   -> Dữ liệu để phân tích thị trường: {len(df):,} tin đăng")

    # ==========================================================================
    # NHIỆM VỤ 1: CẤU TRÚC THỊ TRƯỜNG (AI LÀ ÔNG TRÙM?)
    # Mục tiêu: Xác định Hãng xe (Brand) chiếm thị phần lớn nhất
    # ==========================================================================
    print("   -> Đang tạo Biểu đồ Thị phần Hãng xe...")
    
    # Gom nhóm dữ liệu cho Treemap
    market_share = df.groupby(['brand', 'model']).size().reset_index(name='count')
    market_share_filtered = market_share[market_share['count'] > 5]

    # --- 1.1 Static (Báo cáo) - ĐÃ SỬA THEO YÊU CẦU ---
    # Vẽ Bar Chart thể hiện Top 15 Hãng xe (Brand) thay vì Model
    plt.figure(figsize=(12, 8))
    
    # Lấy Top 15 Hãng xe phổ biến nhất
    top_brands = df['brand'].value_counts().head(15).sort_values(ascending=True)
    
    # Vẽ biểu đồ
    # Dùng màu sắc từ config nếu có, không thì dùng palette mặc định
    colors = [config.BRAND_COLORS.get(brand, '#95a5a6') for brand in top_brands.index]
    
    bars = plt.barh(top_brands.index, top_brands.values, color=colors)
    
    plt.title('Top 15 Hãng xe (Brand) thống trị thị trường xe cũ', fontsize=14, fontweight='bold')
    plt.xlabel('Số lượng tin đăng bán')
    plt.ylabel('Hãng xe')
    plt.grid(axis='x', alpha=0.3)
    
    # Thêm số liệu vào đầu cột
    for i, v in enumerate(top_brands.values):
        plt.text(v + 10, i, str(v), va='center', fontweight='bold', fontsize=10)

    # Chú thích Insight 
    top_1_brand = top_brands.index[-1] # Lấy hãng đứng cuối (vì sort ascending)
    plt.text(top_brands.values[-1] * 0.7, 0, f"Hãng thống trị: {top_1_brand}", 
             fontsize=12, color='red', fontweight='bold', 
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='red'))

    utils.save_static_plot(plt, 'pair_b_static_market_structure.png')
    plt.close()

    # --- 1.2 Interactive (Web - Treemap) ---
    # Treemap vẫn giữ nguyên vì nó thể hiện rất tốt phân cấp Hãng -> Model [cite: 30]
    print("      + Tạo bản Interactive Treemap...")
    
    fig_tree = px.treemap(
        market_share_filtered,
        path=[px.Constant("Thị Trường"), 'brand', 'model'], 
        values='count',
        color='brand', # Tô màu theo Hãng để đồng bộ
        color_discrete_map=config.BRAND_COLORS,
        title='Cấu trúc Thị trường: Hãng xe và các Dòng xe chủ lực',
        hover_data=['count']
    )
    fig_tree.update_traces(textinfo="label+value") 
    fig_tree.update_layout(margin=dict(t=50, l=10, r=10, b=10))
    
    utils.save_interactive_plot(fig_tree, 'pair_b_interactive_treemap.json')


    # ==========================================================================
    # NHIỆM VỤ 2: BAR CHART - PHÂN BỐ VÙNG MIỀN
    # Mục tiêu: Đánh giá nguồn cung theo địa lý [cite: 39]
    # ==========================================================================
    print("   -> Đang tạo Bar Chart (Phân bố Vùng miền)...")
    
    loc_counts = df['location'].value_counts().head(15)
    loc_data = pd.DataFrame({'Tỉnh/Thành': loc_counts.index, 'Số lượng': loc_counts.values})
    
    # --- 2.1 Static (Báo cáo) ---
    plt.figure(figsize=(12, 7))
    sns.barplot(data=loc_data, x='Số lượng', y='Tỉnh/Thành', palette='viridis')
    
    plt.title('Top 15 Khu vực có nguồn cung xe cũ lớn nhất', fontsize=14, fontweight='bold')
    plt.xlabel('Số lượng tin đăng')
    plt.ylabel(None)
    plt.grid(axis='x', alpha=0.3)
    
    utils.save_static_plot(plt, 'pair_b_static_region_dist.png')
    plt.close()

    # --- 2.2 Interactive (Web) ---
    print("      + Tạo bản Interactive Bar Chart...")
    fig_bar = px.bar(
        loc_data,
        x='Số lượng', y='Tỉnh/Thành', orientation='h',
        title='Phân bố Nguồn cung theo Vùng miền (Tương tác)',
        text='Số lượng', color='Số lượng', color_continuous_scale='Viridis'
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
    
    utils.save_interactive_plot(fig_bar, 'pair_b_interactive_region.json')

    print("✅ Hoàn thành Cặp B (Đã cập nhật theo Brand)!")