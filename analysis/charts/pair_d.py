import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import pandas as pd
import numpy as np

# Import module nội bộ
from analysis import utils, config

def prepare_data_overview(df):
    """
    Chuẩn bị dữ liệu cho Pair D (Tổng quan)
    """
    df_clean = df.copy()
    
    # Chuẩn hóa tên Hãng
    df_clean['brand'] = df_clean['brand'].fillna('Unknown').astype(str).str.title()
    
    # Chuyển giá sang Triệu VNĐ
    df_clean['price_million'] = pd.to_numeric(df_clean['price'], errors='coerce') / 1e6
    
    # --- ĐIỀU CHỈNH LOGIC LỌC GIÁ (MỚI) ---
    # Chỉ lấy xe có giá từ 50 triệu đến 10 tỷ đồng
    # Giúp biểu đồ tập trung vào phân khúc phổ thông, dễ nhìn hơn
    df_clean = df_clean[
        (df_clean['price_million'] >= 50) & 
        (df_clean['price_million'] <= 10000)
    ]
    
    return df_clean

def run_analysis(df_master):
    print("\n--- [CẶP D] TỔNG QUAN THỊ TRƯỜNG (TOP BRANDS & PRICE DIST) ---")
    
    df = prepare_data_overview(df_master)
    print(f"   -> Dữ liệu phân tích Pair D (Đã lọc giá hợp lý): {len(df):,} dòng")

    # ==========================================================================
    # BIỂU ĐỒ 1: BAR CHART - TOP 10 HÃNG XE PHỔ BIẾN NHẤT
    # ==========================================================================
    print("   -> [1/2] Đang tạo biểu đồ Top 10 Hãng xe...")
    
    top_brands = df['brand'].value_counts().head(10).sort_values(ascending=True)
    
    # --- Static (Báo cáo) ---
    plt.figure(figsize=(12, 7))
    colors = sns.color_palette("Blues", len(top_brands))
    bars = plt.barh(top_brands.index, top_brands.values, color=colors)
    
    plt.title('Top 10 Hãng xe có số lượng tin đăng nhiều nhất', fontsize=14, fontweight='bold')
    plt.xlabel('Số lượng tin')
    plt.grid(axis='x', alpha=0.3)
    
    for i, v in enumerate(top_brands.values):
        plt.text(v + 10, i, str(v), va='center', fontsize=10)
        
    utils.save_static_plot(plt, 'pair_d_static_top_brands.png')
    plt.close()

    # --- Interactive (Web) ---
    df_top = top_brands.reset_index()
    df_top.columns = ['Brand', 'Count']
    
    fig_bar = px.bar(
        df_top, 
        x='Count', 
        y='Brand', 
        orientation='h', 
        text='Count',
        title='Top 10 Hãng Xe Phổ Biến Nhất',
        color='Count',
        color_continuous_scale='Blues'
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
    utils.save_interactive_plot(fig_bar, 'pair_d_interactive_top_brands.json')


    # ==========================================================================
    # BIỂU ĐỒ 2: HISTOGRAM - PHÂN PHỐI GIÁ TOÀN THỊ TRƯỜNG
    # ==========================================================================
    print("   -> [2/2] Đang tạo biểu đồ Phân phối giá...")
    
    # Lấy mẫu
    df_sample = df.sample(min(20000, len(df)), random_state=42)
    
    # --- Static ---
    plt.figure(figsize=(12, 6))
    # Dùng log scale nếu dữ liệu quá chênh lệch (tùy chọn), ở đây dùng thường cho dễ hiểu
    sns.histplot(df_sample['price_million'], bins=50, kde=True, color='#2ca02c', edgecolor='white')
    
    plt.title('Phân phối giá bán xe cũ (Phân khúc 50Tr - 10 Tỷ)', fontsize=14, fontweight='bold')
    plt.xlabel('Giá xe (Triệu VNĐ)')
    plt.ylabel('Số lượng')
    plt.xlim(0, 5000) # Zoom vào khoảng 0 - 5 tỷ cho dễ nhìn nhất
    plt.grid(axis='y', alpha=0.3)
    
    utils.save_static_plot(plt, 'pair_d_static_price_dist_overview.png')
    plt.close()

    # --- Interactive ---
    fig_hist = px.histogram(
        df_sample, 
        x='price_million', 
        nbins=60,
        title='Phân Phối Giá Bán Toàn Thị Trường (Zoom 0 - 5 Tỷ)',
        labels={'price_million': 'Giá (Triệu VNĐ)'},
        color_discrete_sequence=['#2ca02c']
    )
    # Giới hạn trục X trên Web để dễ nhìn (0 - 5000 Triệu)
    fig_hist.update_xaxes(range=[0, 5000])
    
    median_price = df['price_million'].median()
    fig_hist.add_vline(x=median_price, line_dash="dash", line_color="red", 
                       annotation_text=f"Trung vị: {median_price:.0f} Tr")
    
    utils.save_interactive_plot(fig_hist, 'pair_d_interactive_price_dist_overview.json')

    print("✅ Hoàn thành Cặp D (Đã điều chỉnh giá hợp lý)!")