import os
import glob
import json
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG & ĐƯỜNG DẪN
# ==============================================================================
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CLEANED_DIR = DATA_DIR / "cleaned"
MASTER_DIR = DATA_DIR / "master"
ANALYSIS_OUTPUT_DIR = PROJECT_ROOT / "analysis" / "output"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

st.set_page_config(
    page_title="Car Market Analytics Pro",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS (GIAO DIỆN PREMIUM) ---
st.markdown("""
<style>
    /* 1. Fix lỗi bị che content & tăng khoảng cách */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
    }
    
    /* 2. Style cho Metric Card (Số liệu KPI) */
    div[data-testid="stMetric"] {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
    div[data-testid="stMetricLabel"] {font-size: 1rem; color: #6c757d;}
    div[data-testid="stMetricValue"] {font-size: 1.8rem; color: #0d6efd; font-weight: 700;}

    /* 3. Style cho Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 15px;
        border-bottom: 2px solid #dee2e6;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        font-weight: 600;
        font-size: 1.1rem;
        border-radius: 8px 8px 0 0;
        background-color: transparent;
    }
    .stTabs [aria-selected="true"] {
        background-color: #e7f1ff;
        color: #0d6efd;
        border-bottom: 2px solid #0d6efd;
    }

    /* 4. Style cho Header */
    h1 { color: #212529; font-weight: 800; }
    h2, h3 { color: #343a40; }
    
    /* 5. Sidebar tinh chỉnh */
    section[data-testid="stSidebar"] {
        background-color: #f8f9fa;
        border-right: 1px solid #dee2e6;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# 2. HÀM HỖ TRỢ (HELPERS)
# ==============================================================================
def to_numeric_safe(s):
    if s is None: return pd.Series([], dtype="float64")
    ss = s.astype(str).str.replace(r"[^\d\.]", "", regex=True)
    ss = ss.replace({"": np.nan, "nan": np.nan, "None": np.nan})
    return pd.to_numeric(ss, errors="coerce")

def format_vnd(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)): return "N/A"
    try: x = float(x)
    except: return str(x)
    if x >= 1e9: return f"{x/1e9:.2f} Tỷ"
    if x >= 1e6: return f"{x/1e6:.0f} Tr"
    return f"{x:,.0f} đ"

def find_latest_file(pattern: str):
    files = glob.glob(pattern)
    if not files: return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]

@st.cache_data(show_spinner=False)
def load_and_normalize_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["year"] = to_numeric_safe(df["year"]).fillna(0).astype(int)
    df["price"] = to_numeric_safe(df["price"])
    df["mileage"] = to_numeric_safe(df["mileage"]).fillna(0)
    for col in ["brand", "model", "location", "source"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str).str.title()
    df["price_vis"] = df["price"] / 1e6
    return df

def render_json_chart(filename, title=None, height=480):
    """Render chart với container đẹp hơn"""
    json_path = ANALYSIS_OUTPUT_DIR / filename
    
    # Tạo khung chứa chart
    with st.container():
        if title:
            st.markdown(f"##### {title}")
            
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    fig_json = json.load(f)
                    if isinstance(fig_json, dict) and "data" in fig_json:
                        fig = go.Figure(fig_json)
                        # Tinh chỉnh margin để chart thoáng hơn
                        fig.update_layout(
                            height=height, 
                            margin=dict(l=20, r=20, t=40, b=20),
                            paper_bgcolor='rgba(0,0,0,0)', # Nền trong suốt
                            plot_bgcolor='rgba(0,0,0,0)'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else: st.json(fig_json)
            except Exception as e: st.error(f"Lỗi file {filename}: {e}")
        else:
            st.warning(f"⚠️ Chưa có dữ liệu cho biểu đồ này (`{filename}`)")


# ==============================================================================
# 3. SIDEBAR: DATA LOADING
# ==============================================================================
st.sidebar.markdown("## ⚙️ Cấu Hình Dữ Liệu")
latest_master = find_latest_file(str(MASTER_DIR / "master_dataset_final_*.csv"))

if latest_master:
    df = load_and_normalize_data(latest_master)
    file_name = Path(latest_master).name
    file_date = datetime.fromtimestamp(os.path.getmtime(latest_master)).strftime('%d/%m/%Y %H:%M')
    
    st.sidebar.success(f"✅ Đã tải dữ liệu thành công!")
    with st.sidebar.expander("ℹ️ Thông tin file", expanded=True):
        st.write(f"**Tên file:** `{file_name}`")
        st.write(f"**Cập nhật:** {file_date}")
        st.write(f"**Dung lượng:** {len(df):,} dòng")
else:
    st.sidebar.error("❌ LỖI: Không tìm thấy Master Dataset!")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.info("💡 **Mẹo:** Dùng Tab 'Kiểm Tra Dữ Liệu' để soi lỗi chi tiết từng dòng.")


# ==============================================================================
# 4. MAIN TABS (GIAO DIỆN CHÍNH)
# ==============================================================================
# Icon tab cho sinh động
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Dashboard Tổng Quan", 
    "📊 Báo Cáo Phân Tích", 
    "🔍 Kiểm Tra Dữ Liệu", 
    "📂 Kho Tài Nguyên"
])

# --- TAB 1: DASHBOARD ---
with tab1:
    st.markdown("### 🚗 Dashboard Tổng Quan Thị Trường")
    
    # ... (Phần KPI giữ nguyên) ...
    
    st.markdown("###") # Khoảng cách
    
    # Load 2 biểu đồ từ Pair D (JSON)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**🏆 Top 10 Hãng Xe Phổ Biến**")
        render_json_chart("pair_d_interactive_top_brands.json") # File từ pair_d
        
    with c2:
        st.markdown("**💰 Phân Phối Giá Bán (Toàn thị trường)**")
        render_json_chart("pair_d_interactive_price_dist_overview.json") # File từ pair_d


# --- TAB 2: ANALYSIS REPORTS ---
with tab2:
    st.markdown("### 📊 Báo Cáo Phân Tích Chuyên Sâu")
    st.caption("Các biểu đồ dưới đây được tải từ kết quả chạy `analysis/run_all.py`.")
    
    # Sub-tabs cho gọn
    t_fin, t_mkt, t_tec = st.tabs(["💰 Tài Chính (Pair A)", "🌏 Thị Trường (Pair B)", "⚙️ Kỹ Thuật (Pair C)"])
    
    with t_fin:
        col_a1, col_a2 = st.columns(2)
        with col_a1: render_json_chart("pair_a_interactive_price_dist.json", "1. Phân Phối Giá Chi Tiết")
        with col_a2: render_json_chart("pair_a_interactive_brand_price.json", "2. So Sánh Khoảng Giá Hãng")
        st.divider()
        render_json_chart("pair_a_interactive_price_odo.json", "3. Tương Quan Giá & Khấu Hao (Scatter)", height=550)
        
    with t_mkt:
        render_json_chart("pair_b_interactive_treemap.json", "4. Cấu Trúc Thị Phần (Treemap)", height=650)
        st.divider()
        render_json_chart("pair_b_interactive_region.json", "5. Phân Bố Nguồn Cung Theo Vùng")
        
    with t_tec:
        col_c1, col_c2 = st.columns(2)
        with col_c1: 
            render_json_chart("pair_c_interactive_heatmap.json", "6. Ma Trận Tương Quan (Heatmap)")
            st.info("ℹ️ **Giải thích:** Màu đỏ đậm = Tương quan dương mạnh. Màu xanh đậm = Tương quan âm mạnh.")
        with col_c2: 
            render_json_chart("pair_c_interactive_odo_analysis.json", "7. Phân Tích Xe Lướt vs Xe Cày")
            st.info("ℹ️ **Giải thích:** Xe lướt (đi ít, giá cao) nằm góc trái trên. Xe cày (đi nhiều, giá rẻ) nằm góc phải dưới.")
            
        st.markdown("---")
        col_c3, col_c4 = st.columns(2)
        with col_c3: render_json_chart("pair_c_interactive_year_trend.json", "8. Xu Hướng Số Lượng Theo Năm")
        with col_c4: render_json_chart("pair_c_interactive_fuel_ratio.json", "9. Tỷ Lệ Nhiên Liệu")


# --- TAB 3: DATA CHECK (DEBUG MODE) ---
with tab3:
    st.markdown("### 🧐 Kiểm Tra Sức Khỏe Dữ Liệu")
    
    # 1. File Selector
    csv_files = sorted(list(MASTER_DIR.glob("*.csv")), key=lambda p: p.stat().st_mtime, reverse=True)
    
    if not csv_files:
        st.warning("⚠️ Không tìm thấy file CSV nào để kiểm tra.")
    else:
        c_sel, c_btn = st.columns([3, 1])
        with c_sel:
            selected_file = st.selectbox(
                "Chọn phiên bản dữ liệu:", 
                csv_files, 
                format_func=lambda p: f"{p.name} ({datetime.fromtimestamp(p.stat().st_mtime).strftime('%d/%m %H:%M')})"
            )
        with c_btn:
            st.write("")
            if st.button("🔄 Reload Data"): st.rerun()

        # Load file debug
        try:
            df_debug = pd.read_csv(selected_file)
            
            # 2. Health Metrics
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Tổng Dòng", f"{len(df_debug):,}")
            m2.metric("Tổng Cột", f"{len(df_debug.columns)}")
            
            dups = df_debug.duplicated().sum()
            m3.metric("Trùng Lặp", f"{dups}", delta="OK" if dups==0 else "Cảnh báo", delta_color="inverse")
            
            null_count = df_debug.isnull().sum().sum()
            null_pct = (null_count / df_debug.size) * 100
            m4.metric("Ô Trống (Null)", f"{null_count:,}", f"{null_pct:.2f}%")
            
            st.divider()
            
            # 3. Chi tiết (Expanders thay cho Tabs con để đỡ rối)
            with st.expander("📋 Xem Bảng Dữ Liệu (Top 1000)", expanded=True):
                st.dataframe(df_debug.head(1000), use_container_width=True, height=400)
                
            col_d1, col_d2 = st.columns(2)
            
            with col_d1:
                with st.expander("ℹ️ Kiểm tra Kiểu Dữ Liệu"):
                    dtype_info = df_debug.dtypes.astype(str).reset_index()
                    dtype_info.columns = ["Tên Cột", "Kiểu Dữ Liệu"]
                    dtype_info["Null Count"] = df_debug.isnull().sum().values
                    st.dataframe(dtype_info, use_container_width=True)
            
            with col_d2:
                with st.expander("📊 Thống Kê Số (Tìm Giá Trị Ảo)"):
                    df_num = df_debug.select_dtypes(include=[np.number])
                    if not df_num.empty:
                        st.dataframe(df_num.describe().T.style.format("{:,.2f}"), use_container_width=True)
                    else:
                        st.warning("Không có cột số nào.")
            
            with st.expander("⚠️ Lọc Nhanh Các Dòng Bị Lỗi (Null)"):
                col_err = st.selectbox("Chọn cột cần soi lỗi:", df_debug.columns)
                err_rows = df_debug[df_debug[col_err].isnull()]
                if err_rows.empty:
                    st.success(f"✅ Cột '{col_err}' sạch sẽ, không có ô trống.")
                else:
                    st.error(f"⚠️ Phát hiện {len(err_rows)} dòng bị Null ở cột '{col_err}':")
                    st.dataframe(err_rows, use_container_width=True)

        except Exception as e:
            st.error(f"Không đọc được file: {e}")


# --- TAB 4: RESOURCES ---
with tab4:
    st.markdown("### 📂 Kho Tài Nguyên Báo Cáo")
    st.caption("Tải xuống các hình ảnh biểu đồ tĩnh (.png) chất lượng cao.")
    
    if ANALYSIS_OUTPUT_DIR.exists():
        imgs = sorted(list(ANALYSIS_OUTPUT_DIR.glob("*.png")))
        if imgs:
            cols = st.columns(4)
            for idx, p in enumerate(imgs):
                with cols[idx % 4]:
                    with st.container():
                        st.image(str(p), caption=p.name, use_container_width=True)
                        with open(p, "rb") as f:
                            st.download_button(
                                label=f"⬇️ Tải {p.name}",
                                data=f,
                                file_name=p.name,
                                mime="image/png",
                                key=f"dl_{idx}"
                            )
        else: st.info("Chưa có ảnh PNG nào.")
    else:
        st.error("Không tìm thấy thư mục output.")