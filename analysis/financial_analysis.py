
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import glob
import os
from datetime import datetime

# Cấu hình style cho biểu đồ đẹp hơn
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("Set2")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 12
plt.rcParams['axes.labelsize'] = 13
plt.rcParams['axes.titlesize'] = 16
plt.rcParams['xtick.labelsize'] = 11
plt.rcParams['ytick.labelsize'] = 11

# Tạo thư mục lưu biểu đồ
os.makedirs('analysis/charts', exist_ok=True)


def load_data():
    """Đọc file master dataset mới nhất"""
    master_folder = 'data/master'
    files = glob.glob(os.path.join(master_folder, "*.csv"))
    
    if not files:
        raise FileNotFoundError("Không tìm thấy file master dataset!")
    
    # Lấy file mới nhất
    latest_file = max(files, key=os.path.getctime)
    print(f"📂 Đang đọc file: {os.path.basename(latest_file)}")
    
    df = pd.read_csv(latest_file, encoding='utf-8-sig')
    print(f"✅ Đã load {len(df):,} dòng dữ liệu")
    
    return df


def prepare_data(df):
    """Chuẩn bị dữ liệu cho phân tích"""
    df = df.copy()
    
    # Lọc bỏ các brand không hợp lệ (là số năm như "2025", "2019", etc.)
    # Chỉ giữ các brand là tên hãng xe thực sự
    valid_brands = ['Toyota', 'Honda', 'Mazda', 'Hyundai', 'Kia', 'Ford', 
                    'Mercedes', 'BMW', 'Audi', 'Vinfast', 'Mitsubishi', 
                    'Nissan', 'Suzuki', 'Lexus', 'Porsche', 'Land Rover', 
                    'MG', 'Peugeot', 'Volvo', 'Subaru', 'Isuzu', 'Chevrolet',
                    'Renault', 'VW', 'Volkswagen', 'Mini', 'Jaguar', 'Infiniti',
                    'Acura', 'Genesis', 'Cadillac', 'Lincoln', 'Bentley', 
                    'Rolls-Royce', 'Maserati', 'Ferrari', 'Lamborghini', 
                    'McLaren', 'Tesla', 'Fiat', 'Opel', 'Skoda', 'Seat', 
                    'Dacia', 'Geely', 'Haval', 'Great Wall', 'Chery', 'BYD',
                    'Other']
    
    # Lọc bỏ brand là số (năm sản xuất bị nhầm)
    df = df[~df['brand'].astype(str).str.isdigit()]
    # Lọc bỏ brand quá ngắn hoặc không hợp lệ
    df = df[df['brand'].str.len() > 2]
    
    # Tính tuổi xe (năm hiện tại - năm sản xuất)
    current_year = datetime.now().year
    df['age'] = current_year - df['year']
    
    # Lọc dữ liệu hợp lệ
    # Tuổi xe phải >= 0 và <= 30 (xe quá cũ hoặc tương lai không hợp lý)
    df = df[(df['age'] >= 0) & (df['age'] <= 30)]
    
    # Chuyển giá từ VNĐ sang triệu VNĐ để dễ đọc
    df['price_million'] = df['price'] / 1_000_000

    # Loại bỏ outlier giá quá cao (trên 99th percentile) để biểu đồ rõ ràng
    price_cap = df['price_million'].quantile(0.99)
    df = df[df['price_million'] <= price_cap]
    
    print(f"📊 Sau khi lọc: {len(df):,} dòng dữ liệu hợp lệ")
    print(f"📊 Tuổi xe: {df['age'].min()} - {df['age'].max()} năm")
    print(f"📊 Giá xe: {df['price_million'].min():.1f} - {df['price_million'].max():.1f} triệu VNĐ")
    print(f"📊 Số hãng xe: {df['brand'].nunique()} hãng")
    
    return df


def plot_histogram_price(df):
    """
    BIỂU ĐỒ 1: HISTOGRAM - PHÂN PHỐI GIÁ XE
    =========================================
    Mục đích: Xem độ tập trung của dữ liệu giá xe
    """
    print("\n" + "="*60)
    print("📊 BIỂU ĐỒ 1: HISTOGRAM - PHÂN PHỐI GIÁ XE")
    print("="*60)
    
    fig, ax = plt.subplots(figsize=(14, 7))

    # Histogram với bins cố định (45) để nhìn rõ phân khúc giá
    n, bins, patches = ax.hist(
        df['price_million'],
        bins=45,
        edgecolor='white',
        alpha=0.8,
        color='#5aa9e6'
    )
    ax.set_xlabel('Giá xe (Triệu VNĐ)')
    ax.set_ylabel('Số lượng xe')
    ax.set_title('Phân phối giá xe cũ – Thị trường tập trung ở phân khúc bình dân', pad=18)
    ax.grid(True, axis='y', alpha=0.35)

    # Thêm đường trung bình và trung vị
    mean_price = df['price_million'].mean()
    median_price = df['price_million'].median()
    ax.axvline(mean_price, color='#d7263d', linestyle='--', linewidth=2, label=f'Giá trung bình: {mean_price:.1f} triệu')
    ax.axvline(median_price, color='#1b998b', linestyle='--', linewidth=2, label=f'Giá trung vị: {median_price:.1f} triệu')
    ax.legend(loc='upper right')

    # Annotation insight
    ax.annotate(
        'Phần lớn xe tập trung ở phân khúc giá thấp –\nthị trường bình dân chiếm ưu thế',
        xy=(median_price, max(n) * 0.8),
        xytext=(median_price * 1.6, max(n) * 0.9),
        arrowprops=dict(arrowstyle='->', color='gray', lw=1.5),
        fontsize=12,
        bbox=dict(boxstyle='round,pad=0.4', fc='white', ec='gray', alpha=0.9)
    )

    plt.tight_layout()
    plt.savefig('analysis/charts/1_histogram_price_distribution.png', dpi=300, bbox_inches='tight')
    print('✅ Đã lưu biểu đồ: analysis/charts/1_histogram_price_distribution.png')
    
    # Phân tích
    print("\n📈 PHÂN TÍCH:")
    print(f"   - Giá trung bình: {mean_price:,.1f} triệu VNĐ")
    print(f"   - Giá trung vị: {median_price:,.1f} triệu VNĐ")
    print(f"   - Giá thấp nhất: {df['price_million'].min():,.1f} triệu VNĐ")
    print(f"   - Giá cao nhất: {df['price_million'].max():,.1f} triệu VNĐ")
    
    # Tìm khoảng giá có nhiều xe nhất
    max_bin_idx = np.argmax(n)
    max_bin_start = bins[max_bin_idx]
    max_bin_end = bins[max_bin_idx + 1]
    print(f"   - Khoảng giá phổ biến nhất: {max_bin_start:.0f} - {max_bin_end:.0f} triệu VNĐ ({n[max_bin_idx]:.0f} xe)")
    
    # Tính skewness
    skewness = df['price_million'].skew()
    print(f"   - Độ lệch (Skewness): {skewness:.2f}")
    if skewness > 1:
        print("     → Phân phối lệch phải: Có nhiều xe giá cao làm nhiễu giá trung bình")
    elif skewness < -1:
        print("     → Phân phối lệch trái: Có nhiều xe giá thấp")
    else:
        print("     → Phân phối gần đối xứng")
    
    plt.show()


def plot_boxplot_price_by_brand(df):
    """
    BIỂU ĐỒ 2: BOXPLOT - SO SÁNH GIÁ THEO HÃNG
    ===========================================
    Mục đích: So sánh khoảng dao động giá giữa các hãng xe
    """
    print("\n" + "="*60)
    print("📊 BIỂU ĐỒ 2: BOXPLOT - SO SÁNH GIÁ THEO HÃNG")
    print("="*60)
    
    # Lọc top 12 hãng có nhiều xe nhất để so sánh
    top_brands = df['brand'].value_counts().head(12).index.tolist()
    df_filtered = df[df['brand'].isin(top_brands)].copy()

    print(f"📊 Phân tích top {len(top_brands)} hãng có nhiều xe nhất")

    fig, ax = plt.subplots(figsize=(16, 8))

    # Sắp xếp theo median tăng dần
    brand_order = df_filtered.groupby('brand')['price_million'].median().sort_values().index

    # Vẽ boxplot (hiển thị outliers mặc định)
    sns.boxplot(
        data=df_filtered,
        x='brand',
        y='price_million',
        order=brand_order,
        ax=ax,
        fliersize=3,
        linewidth=1.2,
        palette="Set3"
    )
    ax.set_xlabel('Hãng xe')
    ax.set_ylabel('Giá xe (Triệu VNĐ)')
    ax.set_title('So sánh mức giá và độ giữ giá giữa các hãng xe cũ', pad=18)
    ax.tick_params(axis='x', rotation=30)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig('analysis/charts/2_boxplot_price_by_brand.png', dpi=300, bbox_inches='tight')
    print('✅ Đã lưu biểu đồ: analysis/charts/2_boxplot_price_by_brand.png')
    
    # Phân tích từng hãng
    print("\n📈 PHÂN TÍCH THEO HÃNG:")
    brand_stats = df_filtered.groupby('brand')['price_million'].agg([
        'count', 'mean', 'median', 'std', 'min', 'max'
    ]).sort_values('median', ascending=False)
    
    for brand in brand_order[:10]:  # Top 10 hãng
        stats = brand_stats.loc[brand]
        iqr = df_filtered[df_filtered['brand'] == brand]['price_million'].quantile(0.75) - \
              df_filtered[df_filtered['brand'] == brand]['price_million'].quantile(0.25)
        
        print(f"\n   🚗 {brand}:")
        print(f"      - Số lượng: {stats['count']:.0f} xe")
        print(f"      - Giá trung bình: {stats['mean']:,.1f} triệu VNĐ")
        print(f"      - Giá trung vị: {stats['median']:,.1f} triệu VNĐ")
        print(f"      - Khoảng giá: {stats['min']:,.1f} - {stats['max']:,.1f} triệu VNĐ")
        print(f"      - IQR (độ biến động): {iqr:,.1f} triệu VNĐ")
        
        # Đánh giá độ ổn định giá
        cv = (stats['std'] / stats['mean']) * 100  # Coefficient of Variation
        if cv < 20:
            stability = "Rất ổn định"
        elif cv < 40:
            stability = "Ổn định"
        else:
            stability = "Biến động lớn"
        print(f"      - Độ ổn định: {stability} (CV: {cv:.1f}%)")
    
    plt.show()


def plot_scatter_age_vs_price(df):
    """
    BIỂU ĐỒ 3: SCATTER PLOT - TUỔI XE VS GIÁ
    ==========================================
    Mục đích: Tìm mối quan hệ giữa tuổi xe và giá (tốc độ khấu hao)
    """
    print("\n" + "="*60)
    print("📊 BIỂU ĐỒ 3: SCATTER PLOT - TUỔI XE VS GIÁ")
    print("="*60)
    
    fig, ax = plt.subplots(figsize=(14, 7))

    # Scatter với alpha thấp để tránh chồng điểm
    sns.regplot(
        data=df,
        x='age',
        y='price_million',
        scatter_kws={'alpha': 0.45, 's': 35, 'edgecolor': 'white'},
        line_kws={'color': '#d7263d', 'lw': 2},
        color='#355070',
        ax=ax
    )

    # Tính đường hồi quy để dùng lại cho phân tích
    z = np.polyfit(df['age'], df['price_million'], 1)
    p = np.poly1d(z)

    ax.set_xlabel('Tuổi xe (Năm)')
    ax.set_ylabel('Giá xe (Triệu VNĐ)')
    ax.set_title('Mối quan hệ giữa Tuổi xe và Giá – Phản ánh tốc độ khấu hao', pad=18)
    ax.grid(True, alpha=0.35)

    # Annotation insight
    max_age = df['age'].max()
    ax.annotate(
        'Xe càng cũ → Giá càng giảm (thể hiện khấu hao)',
        xy=(max_age * 0.6, df['price_million'].quantile(0.6)),
        xytext=(max_age * 0.75, df['price_million'].quantile(0.85)),
        arrowprops=dict(arrowstyle='->', color='gray', lw=1.5),
        fontsize=12,
        bbox=dict(boxstyle='round,pad=0.4', fc='white', ec='gray', alpha=0.9)
    )

    plt.tight_layout()
    plt.savefig('analysis/charts/3_scatter_age_vs_price.png', dpi=300, bbox_inches='tight')
    print('✅ Đã lưu biểu đồ: analysis/charts/3_scatter_age_vs_price.png')
    
    # Phân tích tốc độ khấu hao
    print("\n📈 PHÂN TÍCH TỐC ĐỘ KHẤU HAO:")
    
    # Tính hệ số tương quan
    correlation = df['age'].corr(df['price_million'])
    print(f"   - Hệ số tương quan: {correlation:.3f}")
    if abs(correlation) > 0.7:
        strength = "Mạnh"
    elif abs(correlation) > 0.4:
        strength = "Trung bình"
    else:
        strength = "Yếu"
    print(f"   - Mức độ tương quan: {strength}")
    
    # Tính tốc độ khấu hao trung bình (triệu VNĐ/năm)
    depreciation_rate = -z[0]  # Độ dốc âm = tốc độ mất giá
    print(f"   - Tốc độ khấu hao trung bình: {depreciation_rate:,.1f} triệu VNĐ/năm")
    
    # Phân tích theo từng năm tuổi
    print("\n   📊 Giá trung bình theo tuổi xe:")
    age_groups = df.groupby('age')['price_million'].agg(['mean', 'count']).sort_index()
    for age in range(min(10, len(age_groups))):  # Hiển thị 10 năm đầu
        if age in age_groups.index:
            stats = age_groups.loc[age]
            print(f"      - {age} tuổi: {stats['mean']:,.1f} triệu VNĐ ({stats['count']:.0f} xe)")
    
    # Tìm "món hời" - xe có giá thấp hơn xu hướng
    df['predicted_price'] = p(df['age'])
    df['price_difference'] = df['price_million'] - df['predicted_price']
    bargains = df.nsmallest(10, 'price_difference')[['brand', 'model', 'age', 'price_million', 'price_difference']]
    
    print("\n   💰 Top 10 'Món hời' (xe có giá thấp hơn xu hướng):")
    for idx, row in bargains.iterrows():
        print(f"      - {row['brand']} {row['model']} ({row['age']} tuổi): "
              f"{row['price_million']:,.1f} triệu (thấp hơn {abs(row['price_difference']):,.1f} triệu)")
    
    plt.show()


def main():
    """Hàm chính chạy toàn bộ phân tích"""
    print("="*60)
    print("🚗 PHÂN TÍCH TÀI CHÍNH: GIÁ XE & KHẤU HAO")
    print("="*60)
    print("Cặp A - Nhiệm vụ phân tích tài chính\n")
    
    try:
        # 1. Load dữ liệu
        df = load_data()
        
        # 2. Chuẩn bị dữ liệu
        df = prepare_data(df)
        
        # 3. Vẽ 3 biểu đồ
        plot_histogram_price(df)
        plot_boxplot_price_by_brand(df)
        plot_scatter_age_vs_price(df)
        
        print("\n" + "="*60)
        print("✅ HOÀN TẤT PHÂN TÍCH!")
        print("="*60)
        print("📁 Tất cả biểu đồ đã được lưu trong: analysis/charts/")
        print("   1. 1_histogram_price_distribution.png")
        print("   2. 2_boxplot_price_by_brand.png")
        print("   3. 3_scatter_age_vs_price.png")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

