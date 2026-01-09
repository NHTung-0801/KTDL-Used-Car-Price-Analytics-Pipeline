import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter # Import thêm cái này để format giá tiền chuẩn
from analysis import utils, config

def run_analysis(df):
    print("\n--- [CẶP C] Đang xử lý Phân tích Nâng cao... ---")
    
    # ---------------------------------------------------------
    # 1. Biểu đồ Scatter: Tương quan Năm vs Giá
    # ---------------------------------------------------------
    print("   -> Đang tạo Scatter Plot (Năm vs Giá)...")
    
    plt.figure(figsize=(12, 7))
    
    # LỌC NHIỄU
    df_plot = df[(df['price'] < 10e9) & (df['year'] >= 2005)].copy()
    
    # XỬ LÝ MÀU SẮC
    known_brands = set(config.BRAND_COLORS.keys())
    df_plot['brand_group'] = df_plot['brand'].apply(
        lambda x: x if x in known_brands else 'Other'
    )
    df_plot.sort_values(by='brand_group', key=lambda x: x == 'Other', inplace=True)

    # Vẽ biểu đồ
    ax = sns.scatterplot(
        data=df_plot,
        x='year', 
        y='price',
        hue='brand_group',
        alpha=0.7,
        s=60,
        palette=config.BRAND_COLORS
    )
    
    plt.title('Tương quan Giá xe theo Năm sản xuất', fontsize=14, fontweight='bold')
    plt.xlabel('Năm sản xuất', fontsize=12)
    plt.ylabel('Giá bán', fontsize=12)
    
    # --- FIX LỖI 1: FORMAT TRỤC Y CHUẨN (Dùng FuncFormatter) ---
    def format_price(x, pos):
        if x >= 1e9:
            return f'{x/1e9:.1f} Tỷ'
        return f'{x/1e6:.0f} Tr'
    
    ax.yaxis.set_major_formatter(FuncFormatter(format_price))
    
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
    plt.tight_layout()
    utils.save_static_plot(plt, "pair_c_correlation.png")
    plt.close()
    
    # ---------------------------------------------------------
    # 2. Biểu đồ Boxplot: Mức độ giữ giá
    # ---------------------------------------------------------
    print("   -> Đang tạo Boxplot (Phân bố giá)...")
    plt.figure(figsize=(12, 6))
    
    top_10_brands = df['brand'].value_counts().head(10).index
    df_box = df[df['brand'].isin(top_10_brands)]
    
    # --- FIX LỖI 2: THÊM hue VÀ legend=False ---
    sns.boxplot(
        data=df_box,
        x='brand',
        y='price',
        hue='brand',      # Bắt buộc phải có dòng này ở bản mới
        legend=False,     # Tắt legend thừa đi
        palette="Set2"
    )
    
    plt.yscale('log')
    plt.title('Phân bố khoảng giá của Top 10 Hãng xe (Thang đo Log)', fontsize=14)
    plt.ylabel('Giá xe (Log Scale)')
    plt.xlabel('Hãng xe')
    
    # Format trục Y log scale cho dễ đọc (Option thêm cho đẹp)
    ax = plt.gca()
    ax.yaxis.set_major_formatter(FuncFormatter(format_price))

    utils.save_static_plot(plt, "pair_c_price_distribution.png")
    plt.close()

    print("✅ Hoàn thành Cặp C!")