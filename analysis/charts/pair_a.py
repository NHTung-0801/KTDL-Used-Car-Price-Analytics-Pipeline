import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import numpy as np
import pandas as pd
from datetime import datetime

# Import module nội bộ
from analysis import utils, config


def prepare_data(df):
    """
    Chuẩn bị dữ liệu sạch cho Phân tích Tài chính (Cặp A)

    - Tính tuổi xe
    - Lọc xe quá cũ / giá ảo
    - Chuẩn hóa giá (Triệu VNĐ)
    - Loại bỏ outlier Giá & ODO theo percentile (phục vụ trực quan)
    """
    df_clean = df.copy()

    # 1. Tính tuổi xe
    current_year = datetime.now().year
    df_clean['age'] = current_year - df_clean['year']

    # 2. Lọc dữ liệu hợp lý
    df_clean = df_clean[
        (df_clean['age'] >= 0) &
        (df_clean['age'] <= 30) &
        (df_clean['price'] > 20_000_000)
    ]

    # 3. Chuyển đổi đơn vị giá sang Triệu đồng
    df_clean['price_million'] = df_clean['price'] / 1e6

    # 4. Loại bỏ outlier giá (Top 1%)
    price_cap = df_clean['price_million'].quantile(0.99)
    df_clean = df_clean[df_clean['price_million'] <= price_cap]

    # 5. Loại bỏ outlier ODO (Top 5%) -> QUAN TRỌNG cho Scatter
    if 'mileage' in df_clean.columns:
        odo_cap = df_clean['mileage'].quantile(0.95)
        df_clean = df_clean[df_clean['mileage'] <= odo_cap]

    return df_clean


def run_analysis(df_master):
    """
    Hàm chính được gọi bởi run_all.py
    Vẽ cả biểu đồ TĨNH (báo cáo) và ĐỘNG (dashboard)
    """
    print("\n--- [CẶP A] PHÂN TÍCH TÀI CHÍNH & KHẤU HAO ---")

    # 1. Chuẩn bị dữ liệu
    df = prepare_data(df_master)
    print(f"   -> Dữ liệu sau khi lọc sạch: {len(df):,} dòng")

    # ==========================================================================
    # PHẦN 1: BIỂU ĐỒ TĨNH (MATPLOTLIB / SEABORN)
    # ==========================================================================

    # --- 1.1 Histogram: Phân phối giá ---
    print("   -> [Static] Histogram phân phối giá...")
    plt.figure(figsize=(12, 6))

    sns.histplot(
        df['price_million'],
        bins=50,
        kde=True,
        color='#5aa9e6',
        edgecolor='white'
    )

    mean_val = df['price_million'].mean()
    median_val = df['price_million'].median()

    plt.axvline(mean_val, color='red', linestyle='--', label=f'TB: {mean_val:.0f} Tr')
    plt.axvline(median_val, color='green', linestyle='--', label=f'Trung vị: {median_val:.0f} Tr')

    plt.title('Phân phối Giá xe cũ (Thị trường tập trung phân khúc bình dân)', fontsize=14, fontweight='bold')
    plt.xlabel('Giá xe (Triệu VNĐ)')
    plt.ylabel('Số lượng tin')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)

    utils.save_static_plot(plt, 'pair_a_static_price_dist.png')
    plt.close()

    # --- 1.2 Scatter: Tuổi xe vs Giá (Khấu hao) ---
    print("   -> [Static] Scatter khấu hao...")
    plt.figure(figsize=(12, 6))

    sns.regplot(
        data=df,
        x='age',
        y='price_million',
        scatter_kws={'alpha': 0.3, 's': 15, 'color': '#355070'},
        line_kws={'color': '#d7263d', 'linewidth': 2}
    )

    plt.title('Tốc độ mất giá của xe theo thời gian (Khấu hao)', fontsize=14, fontweight='bold')
    plt.xlabel('Tuổi xe (Năm)')
    plt.ylabel('Giá bán (Triệu VNĐ)')
    plt.grid(True, alpha=0.3)

    plt.annotate(
        'Xe càng cũ → Giá càng giảm',
        xy=(15, df['price_million'].median()),
        xytext=(20, df['price_million'].quantile(0.75)),
        arrowprops=dict(facecolor='black', shrink=0.05),
        fontsize=11
    )

    utils.save_static_plot(plt, 'pair_a_static_depreciation.png')
    plt.close()

    # --- 1.3 Boxplot: So sánh giá theo Hãng ---
    print("   -> [Static] Boxplot theo hãng...")
    plt.figure(figsize=(14, 7))

    top_brands = df['brand'].value_counts().head(12).index
    df_top = df[df['brand'].isin(top_brands)]

    order = df_top.groupby('brand')['price_million'].median().sort_values().index

    sns.boxplot(
        data=df_top,
        x='brand',
        y='price_million',
        order=order,
        palette='Set3'
    )

    plt.title('Khoảng giá giao dịch của Top 12 Hãng xe phổ biến', fontsize=14, fontweight='bold')
    plt.xlabel('Hãng xe')
    plt.ylabel('Giá (Triệu VNĐ)')
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)

    utils.save_static_plot(plt, 'pair_a_static_brand_price.png')
    plt.close()

    # ==========================================================================
    # PHẦN 2: BIỂU ĐỒ ĐỘNG (PLOTLY)
    # ==========================================================================

    # --- 2.1 Interactive Histogram ---
    print("   -> [Interactive] Histogram JSON...")
    fig_hist = px.histogram(
        df,
        x='price_million',
        nbins=60,
        title='Phân phối Giá xe (Interactive)',
        labels={'price_million': 'Giá (Triệu VNĐ)'},
        color_discrete_sequence=['#5aa9e6']
    )

    fig_hist.add_vline(
        x=mean_val,
        line_dash="dash",
        line_color="red",
        annotation_text="Trung bình"
    )

    fig_hist.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    utils.save_interactive_plot(fig_hist, 'pair_a_interactive_price_dist.json')

    # --- 2.2 Interactive Scatter: Giá vs ODO ---
    print("   -> [Interactive] Scatter Giá vs ODO...")

    df_sample = df.sample(min(3000, len(df)), random_state=42)

    fig_scatter = px.scatter(
        df_sample,
        x='mileage',
        y='price_million',
        color='brand',
        title='Tương quan Giá vs Số KM đã đi (Theo hãng)',
        labels={
            'mileage': 'Số Km (ODO)',
            'price_million': 'Giá (Triệu VNĐ)'
        },
        hover_data=['model', 'year', 'location'],
        color_discrete_map=config.BRAND_COLORS
    )

    fig_scatter.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    utils.save_interactive_plot(fig_scatter, 'pair_a_interactive_price_odo.json')
    

    # --- 2.3 Interactive Boxplot ---
    print("   -> [Interactive] Boxplot JSON...")
    fig_box = px.box(
        df_top,
        x='brand',
        y='price_million',
        color='brand',
        title='Phân bố khoảng giá (So sánh Hãng)',
        labels={'brand': 'Hãng xe', 'price_million': 'Giá (Triệu VNĐ)'},
        color_discrete_map=config.BRAND_COLORS
    )

    fig_box.update_layout(showlegend=False, margin=dict(l=20, r=20, t=40, b=20))
    utils.save_interactive_plot(fig_box, 'pair_a_interactive_brand_price.json')

    print("✅ Hoàn thành Cặp A!")
    print("   - 3 biểu đồ PNG (báo cáo)")
    print("   - 3 file JSON (dashboard)")
