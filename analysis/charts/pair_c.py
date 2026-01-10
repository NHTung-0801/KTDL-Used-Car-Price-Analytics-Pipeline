import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import pandas as pd
import numpy as np
from matplotlib.ticker import FuncFormatter

# Import module nội bộ
from analysis import utils, config

def prepare_data_technical(df):
    """
    Chuẩn bị dữ liệu Kỹ thuật:
    - Cần các số liệu: Giá, Tuổi, ODO (Mileage), Nhiên liệu (Fuel)
    """
    df_clean = df.copy()
    
    # 1. Chuyển đổi số liệu
    df_clean['price_million'] = pd.to_numeric(df_clean['price'], errors='coerce') / 1e6
    df_clean['year'] = pd.to_numeric(df_clean['year'], errors='coerce')
    df_clean['mileage'] = pd.to_numeric(df_clean['mileage'], errors='coerce')
    
    # 2. Tính tuổi xe
    current_year = pd.Timestamp.now().year
    df_clean['age'] = current_year - df_clean['year']
    
    # 3. Lọc nhiễu
    df_clean = df_clean[
        (df_clean['price_million'] > 20) & (df_clean['price_million'] < 10000) &
        (df_clean['mileage'] > 0) & (df_clean['mileage'] < 500000) &
        (df_clean['age'] >= 0)
    ]
    
    # 4. Chuẩn hóa Nhiên liệu (Gom nhóm nếu cần)
    if 'fuel' in df_clean.columns:
        df_clean['fuel'] = df_clean['fuel'].fillna('Khác').astype(str).str.title()
        # Gom nhóm nhỏ vào 'Khác' nếu cần thiết
    
    return df_clean

def format_price_vn(x, pos=None):
    if x >= 1000: return f'{x/1000:.1f} Tỷ'
    return f'{x:.0f} Tr'

def run_analysis(df_master):
    print("\n--- [CẶP C] PHÂN TÍCH KỸ THUẬT & QUY LUẬT (CẬP NHẬT) ---")
    
    df = prepare_data_technical(df_master)
    print(f"   -> Dữ liệu sạch: {len(df):,} xe")

    # ==========================================================================
    # BIỂU ĐỒ 1: HEATMAP - QUY LUẬT ẢNH HƯỞNG (KỸ THUẬT) - GIỮ NGUYÊN
    # ==========================================================================
    print("   -> [1/4] Đang tạo Heatmap...")
    corr_data = df[['price_million', 'age', 'mileage']].corr()
    
    # Static
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_data, annot=True, fmt=".2f", cmap='RdBu_r', vmin=-1, vmax=1, 
                linewidths=1, annot_kws={"size": 14, "weight": "bold"})
    plt.title('Ma trận tương quan: Tuổi - ODO - Giá', fontsize=15, fontweight='bold')
    utils.save_static_plot(plt, 'pair_c_static_heatmap.png')
    plt.close()

    # Interactive
    fig_heat = px.imshow(corr_data, text_auto=".2f", aspect="auto", color_continuous_scale='RdBu_r',
                         title='Ma trận tương quan: Yếu tố nào làm xe mất giá nhất?')
    utils.save_interactive_plot(fig_heat, 'pair_c_interactive_heatmap.json')


    # ==========================================================================
    # BIỂU ĐỒ 2: SCATTER ODO - PHÂN TÍCH XE LƯỚT (KỸ THUẬT) - GIỮ NGUYÊN
    # ==========================================================================
    print("   -> [2/4] Đang tạo Scatter ODO...")
    df_sample = df.sample(min(3000, len(df)), random_state=42)
    top_brands = df['brand'].value_counts().head(6).index
    df_sample['brand_group'] = df_sample['brand'].apply(lambda x: x if x in top_brands else 'Other')

    # Static
    plt.figure(figsize=(12, 7))
    sns.scatterplot(data=df_sample, x='mileage', y='price_million', hue='brand_group', alpha=0.7, s=50, palette='bright')
    plt.title('Tương quan Giá và ODO (Số Km đã đi)', fontsize=15, fontweight='bold')
    plt.xlabel('Số Km (ODO)'); plt.ylabel('Giá (Triệu VNĐ)')
    plt.axvline(x=20000, color='green', linestyle='--'); plt.text(5000, df_sample['price_million'].max()*0.9, 'Xe Lướt', color='green', fontweight='bold')
    utils.save_static_plot(plt, 'pair_c_static_odo_analysis.png')
    plt.close()

    # Interactive
    fig_odo = px.scatter(df_sample, x='mileage', y='price_million', color='brand', size='year',
                         title='Phân tích Đa chiều: Giá - ODO - Đời xe', color_discrete_map=config.BRAND_COLORS)
    utils.save_interactive_plot(fig_odo, 'pair_c_interactive_odo_analysis.json')


    # ==========================================================================
    # BIỂU ĐỒ 3: LINE CHART - XU HƯỚNG SỐ LƯỢNG XE THEO NĂM (MẪU MỚI)
    # ==========================================================================
    print("   -> [3/4] Đang tạo Line Chart (Xu hướng Năm)...")
    
    # Tính toán số lượng xe theo từng năm
    year_counts = df['year'].value_counts().sort_index()
    year_df = pd.DataFrame({'Năm': year_counts.index, 'Số lượng': year_counts.values})
    
    # Static (Báo cáo) - Dạng Line có chấm tròn (Marker)
    plt.figure(figsize=(12, 6))
    plt.plot(year_df['Năm'], year_df['Số lượng'], marker='o', linestyle='-', color='#2ca02c', linewidth=2, markersize=8)
    
    plt.title('Xu hướng Số lượng xe cũ theo Năm sản xuất', fontsize=15, fontweight='bold')
    plt.xlabel('Năm sản xuất', fontsize=12)
    plt.ylabel('Số lượng xe', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.fill_between(year_df['Năm'], year_df['Số lượng'], color='#2ca02c', alpha=0.1) # Tô màu vùng dưới
    
    utils.save_static_plot(plt, 'pair_c_static_year_trend.png')
    plt.close()

    # Interactive (Web)
    fig_year = px.line(year_df, x='Năm', y='Số lượng', markers=True, 
                       title='Phân bố xe theo Năm sản xuất (Tương tác)',
                       labels={'Năm': 'Năm Sản Xuất', 'Số lượng': 'Số Tin Đăng'})
    fig_year.update_traces(line_color='#2ca02c', line_width=3)
    utils.save_interactive_plot(fig_year, 'pair_c_interactive_year_trend.json')


    # ==========================================================================
    # BIỂU ĐỒ 4: PIE/BAR CHART - TỶ LỆ NHIÊN LIỆU (MẪU MỚI)
    # ==========================================================================
    print("   -> [4/4] Đang tạo Biểu đồ Nhiên liệu...")
    
    if 'fuel' in df.columns:
        fuel_counts = df['fuel'].value_counts()
        
        # Static (Báo cáo) - Dùng Pie Chart cho trực quan tỷ lệ
        plt.figure(figsize=(8, 8))
        colors = sns.color_palette('pastel')[0:len(fuel_counts)]
        
        # Vẽ Pie Chart
        plt.pie(fuel_counts, labels=fuel_counts.index, autopct='%1.1f%%', startangle=140, colors=colors, 
                wedgeprops={'edgecolor': 'white', 'linewidth': 2}) # Viền trắng cho đẹp
        
        plt.title('Tỷ lệ các loại Nhiên liệu trên thị trường', fontsize=15, fontweight='bold')
        utils.save_static_plot(plt, 'pair_c_static_fuel_ratio.png')
        plt.close()

        # Interactive (Web) - Dùng Pie Chart tương tác
        fig_fuel = px.pie(names=fuel_counts.index, values=fuel_counts.values, 
                          title='Tỷ lệ Nhiên liệu (Tương tác)',
                          color_discrete_sequence=px.colors.qualitative.Pastel)
        fig_fuel.update_traces(textposition='inside', textinfo='percent+label')
        utils.save_interactive_plot(fig_fuel, 'pair_c_interactive_fuel_ratio.json')
    else:
        print("      ⚠️ Không tìm thấy cột 'fuel', bỏ qua biểu đồ này.")

    print("✅ Hoàn thành Cặp C (Đã cập nhật biểu đồ Năm & Nhiên liệu)!")