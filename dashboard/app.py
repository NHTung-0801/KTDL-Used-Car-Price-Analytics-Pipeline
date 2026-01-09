# dashboard/app.py
import os
import glob
import json
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px


# =========================
# Paths
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CLEANED_DIR = DATA_DIR / "cleaned"
MASTER_DIR = DATA_DIR / "master"
ANALYSIS_OUTPUT_DIR = PROJECT_ROOT / "analysis" / "output"


# =========================
# Page config
# =========================
st.set_page_config(
    page_title="Used Car Analytics Dashboard",
    page_icon="🚗",
    layout="wide",
)


# =========================
# Helpers
# =========================
def to_numeric_safe(s):
    """Convert to numeric safely (remove commas, dots, currency chars...)"""
    if s is None:
        return pd.Series([], dtype="float64")
    ss = s.astype(str)
    ss = ss.str.replace(r"[^\d\.]", "", regex=True)
    ss = ss.replace({"": np.nan, "nan": np.nan, "None": np.nan})
    return pd.to_numeric(ss, errors="coerce")


def format_vnd(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "N/A"
    try:
        x = float(x)
    except Exception:
        return str(x)
    if x >= 1e9:
        return f"{x/1e9:.2f} tỷ"
    if x >= 1e6:
        return f"{x/1e6:.0f} triệu"
    return f"{x:,.0f} đ"


def find_latest_file(pattern: str):
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


@st.cache_data(show_spinner=False)
def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ensure cols exist
    for col in ["brand", "model", "year", "price", "mileage", "fuel", "location", "color", "source", "crawl_date"]:
        if col not in df.columns:
            df[col] = np.nan

    # normalize text
    for col in ["brand", "model", "fuel", "location", "color", "source"]:
        df[col] = df[col].astype(str).replace({"nan": np.nan}).fillna("Unknown")

    # numeric
    df["year"] = to_numeric_safe(df["year"]).astype("Int64")
    df["price"] = to_numeric_safe(df["price"])
    df["mileage"] = to_numeric_safe(df["mileage"])

    # date
    df["crawl_date"] = pd.to_datetime(df["crawl_date"], errors="coerce")

    return df


def clamp_iqr(series: pd.Series) -> pd.Series:
    """Clamp outliers using IQR method"""
    s = series.dropna()
    if len(s) < 10:
        return series
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    return series.clip(lower=lo, upper=hi)


def apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    out = df.copy()

    # sources
    if f["sources"]:
        out = out[out["source"].isin(f["sources"])]

    # brand
    if f["brands"]:
        out = out[out["brand"].isin(f["brands"])]

    # fuel
    if f["fuels"]:
        out = out[out["fuel"].isin(f["fuels"])]

    # location
    if f["locations"]:
        out = out[out["location"].isin(f["locations"])]

    # colors
    if f["colors"]:
        out = out[out["color"].isin(f["colors"])]

    # year range
    y_min, y_max = f["year_range"]
    out = out[(out["year"].fillna(0) >= y_min) & (out["year"].fillna(0) <= y_max)]

    # price range
    p_min, p_max = f["price_range"]
    out = out[(out["price"] >= p_min) & (out["price"] <= p_max)]

    # mileage range
    m_min, m_max = f["mileage_range"]
    out = out[(out["mileage"].fillna(0) >= m_min) & (out["mileage"].fillna(0) <= m_max)]

    # text search
    q = (f["search_text"] or "").strip().lower()
    if q:
        out = out[
            out["brand"].astype(str).str.lower().str.contains(q, na=False)
            | out["model"].astype(str).str.lower().str.contains(q, na=False)
            | out["location"].astype(str).str.lower().str.contains(q, na=False)
        ]

    return out


def load_analysis_artifacts():
    """Return list of images + json files in analysis/output"""
    if not ANALYSIS_OUTPUT_DIR.exists():
        return [], []
    imgs = []
    jsons = []
    for p in sorted(ANALYSIS_OUTPUT_DIR.glob("*")):
        if p.suffix.lower() in [".png", ".jpg", ".jpeg", ".webp"]:
            imgs.append(p)
        elif p.suffix.lower() == ".json":
            jsons.append(p)
    return imgs, jsons


# =========================
# Sidebar - Data source
# =========================
st.sidebar.title("⚙️ Settings")

use_master = st.sidebar.radio(
    "Chọn nguồn dữ liệu",
    ["Master dataset (khuyên dùng)", "Chọn file cleaned (theo nguồn)"],
    index=0
)

if use_master.startswith("Master"):
    latest_master = find_latest_file(str(MASTER_DIR / "master_dataset_final_*.csv"))
    if latest_master is None:
        st.error("Không tìm thấy master_dataset_final_*.csv trong data/master/")
        st.stop()

    st.sidebar.success(f"📂 Master: {Path(latest_master).name}")
    df = load_csv(latest_master)

else:
    cleaned_files = sorted(CLEANED_DIR.glob("*_cleaned_*.csv"))
    if not cleaned_files:
        st.error("Không có file cleaned trong data/cleaned/")
        st.stop()
    choice = st.sidebar.selectbox("Chọn file cleaned", cleaned_files, format_func=lambda p: p.name)
    st.sidebar.success(f"📂 Cleaned: {choice.name}")
    df = load_csv(str(choice))

df = normalize_df(df)

st.sidebar.divider()


# =========================
# Tabs
# =========================
tab1, tab2 = st.tabs(["📈 Dashboard", "📊 Analysis (Pair A/B/C)"])


# =========================
# TAB 1: Dashboard
# =========================
with tab1:
    st.title("🚗 Used Car Price Analytics Dashboard")
    st.caption("Lọc dữ liệu + visualization nhanh (Plotly). Nguồn: master/cleaned CSV.")

    # dataset stats
    c0, c1, c2, c3 = st.columns(4)
    c0.metric("Rows", f"{len(df):,}")
    c1.metric("Sources", df["source"].nunique())
    c2.metric("Brands", df["brand"].nunique())
    c3.metric("Locations", df["location"].nunique())

    # -------------------------
    # Filters
    # -------------------------
    sources = sorted(df["source"].dropna().unique().tolist())
    brands = sorted(df["brand"].dropna().unique().tolist())
    fuels = sorted(df["fuel"].dropna().unique().tolist())
    locations = sorted(df["location"].dropna().unique().tolist())
    colors = sorted(df["color"].dropna().unique().tolist())

    year_min = int(np.nanmin(df["year"].astype("float64"))) if df["year"].notna().any() else 2000
    year_max = int(np.nanmax(df["year"].astype("float64"))) if df["year"].notna().any() else 2026

    price_min = float(np.nanmin(df["price"])) if df["price"].notna().any() else 0.0
    price_max = float(np.nanmax(df["price"])) if df["price"].notna().any() else 1.0

    mileage_min = float(np.nanmin(df["mileage"])) if df["mileage"].notna().any() else 0.0
    mileage_max = float(np.nanmax(df["mileage"])) if df["mileage"].notna().any() else 1.0

    st.sidebar.subheader("🔎 Filters")

    selected_sources = st.sidebar.multiselect("Source", sources, default=sources)
    selected_brands = st.sidebar.multiselect("Brand", brands, default=[])
    selected_fuels = st.sidebar.multiselect("Fuel", fuels, default=[])
    selected_locations = st.sidebar.multiselect("Location", locations, default=[])
    selected_colors = st.sidebar.multiselect("Color", colors, default=[])

    year_range = st.sidebar.slider("Year range", year_min, year_max, (year_min, year_max))
    price_range = st.sidebar.slider("Price range (VND)", float(price_min), float(price_max), (float(price_min), float(price_max)))
    mileage_range = st.sidebar.slider("Mileage range (km)", float(mileage_min), float(mileage_max), (float(mileage_min), float(mileage_max)))

    search_text = st.sidebar.text_input("Search (brand/model/location)", value="")

    clamp_outliers = st.sidebar.checkbox("Clamp outliers (IQR) cho chart giá/km", value=True)
    top_n = st.sidebar.slider("Top N (brand/model/location)", 5, 30, 10)

    filters = {
        "sources": selected_sources,
        "brands": selected_brands,
        "fuels": selected_fuels,
        "locations": selected_locations,
        "colors": selected_colors,
        "year_range": year_range,
        "price_range": price_range,
        "mileage_range": mileage_range,
        "search_text": search_text,
    }

    df_f = apply_filters(df, filters)

    st.divider()

    # KPI after filter
    k0, k1, k2, k3 = st.columns(4)
    k0.metric("Filtered rows", f"{len(df_f):,}")
    k1.metric("Median price", format_vnd(df_f["price"].median() if len(df_f) else np.nan))
    k2.metric("Median year", int(df_f["year"].median()) if df_f["year"].notna().any() else 0)
    k3.metric("Median mileage", f"{df_f['mileage'].median():,.0f} km" if df_f["mileage"].notna().any() else "N/A")

    # prepare visualization df
    df_vis = df_f.copy()
    if clamp_outliers and len(df_vis):
        df_vis["price_vis"] = clamp_iqr(df_vis["price"])
        df_vis["mileage_vis"] = clamp_iqr(df_vis["mileage"])
    else:
        df_vis["price_vis"] = df_vis["price"]
        df_vis["mileage_vis"] = df_vis["mileage"]

    # =========================
    # Charts row 1
    # =========================
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏷️ Top Brands (Count)")
        top_brand = df_f["brand"].value_counts().head(top_n).reset_index()
        top_brand.columns = ["brand", "count"]
        fig = px.bar(top_brand, x="brand", y="count", title="Top Brands")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("📍 Top Locations (Count)")
        top_loc = df_f["location"].value_counts().head(top_n).reset_index()
        top_loc.columns = ["location", "count"]
        fig = px.bar(top_loc, x="location", y="count", title="Top Locations")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # Charts row 2
    # =========================
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("💰 Price Distribution (Histogram)")
        plot_df = df_vis.copy()
        if len(plot_df) > 50000:
            plot_df = plot_df.sample(50000, random_state=42)
        fig = px.histogram(plot_df, x="price_vis", nbins=60, title="Price Distribution")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        st.subheader("📅 Year Distribution")
        year_counts = df_f["year"].dropna().astype(int).value_counts().sort_index().reset_index()
        year_counts.columns = ["year", "count"]
        fig = px.line(year_counts, x="year", y="count", markers=True, title="Cars by Year")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # Charts row 3
    # =========================
    c5, c6 = st.columns(2)

    with c5:
        st.subheader("⛽ Fuel Type Share")
        fuel_counts = df_f["fuel"].value_counts().reset_index()
        fuel_counts.columns = ["fuel", "count"]
        fig = px.pie(fuel_counts, names="fuel", values="count", title="Fuel Type Share")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c6:
        st.subheader("🔗 Price vs Mileage")
        plot_df = df_vis.copy()
        if len(plot_df) > 15000:
            plot_df = plot_df.sample(15000, random_state=42)
        fig = px.scatter(
            plot_df,
            x="mileage_vis",
            y="price_vis",
            color="source",
            hover_data=["brand", "model", "year", "location"],
            title="Price vs Mileage (sampled if large)",
            opacity=0.6,
        )
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # Charts row 4 - Box by brand
    # =========================
    st.subheader("📦 Price by Brand (Top)")
    top_brands = df_f["brand"].value_counts().head(top_n).index.tolist()
    df_box = df_vis[df_vis["brand"].isin(top_brands)].copy()
    if len(df_box) > 25000:
        df_box = df_box.sample(25000, random_state=42)
    fig = px.box(df_box, x="brand", y="price_vis", points=False, title="Price distribution by Brand (Top)")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # =========================
    # Preview + download
    # =========================
    st.subheader("🔍 Data preview (Top 200)")
    show_cols = ["brand", "model", "year", "price", "mileage", "fuel", "location", "color", "source", "crawl_date"]
    show_cols = [c for c in show_cols if c in df_f.columns]

    st.dataframe(
        df_f[show_cols].sort_values(by=["year", "price"], ascending=[False, False]).head(200),
        use_container_width=True,
        height=420,
    )

    csv_bytes = df_f.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Download filtered CSV", data=csv_bytes, file_name="filtered_dataset.csv", mime="text/csv")

    st.caption("Tip: Nếu chart bị chậm → lọc bớt Brand/Location hoặc bật clamp outliers và giảm Top N.")


# =========================
# TAB 2: Analysis outputs (Pair A/B/C)
# =========================
with tab2:
    st.title("📊 Analysis Outputs (Pair A / B / C)")
    st.caption("Hiển thị các file đã generate từ `python -m analysis.run_all` trong `analysis/output/`.")

    imgs, jsons = load_analysis_artifacts()

    if not imgs and not jsons:
        st.warning("Chưa thấy file trong analysis/output/. Hãy chạy: `python -m analysis.run_all`")
    else:
        # Show images first
        if imgs:
            st.subheader("🖼️ Images")
            # ưu tiên theo thứ tự quen thuộc
            priority = [
                "pair_a_example.png",
                "pair_a_bar_region.png",
                "pair_b_bar_region.png",
                "pair_c_correlation.png",
                "pair_c_price_distribution.png",
            ]
            imgs_sorted = sorted(imgs, key=lambda p: (priority.index(p.name) if p.name in priority else 999, p.name))

            for p in imgs_sorted:
                st.markdown(f"**{p.name}**")
                st.image(str(p), use_container_width=True)
                st.download_button(
                    f"⬇️ Download {p.name}",
                    data=p.read_bytes(),
                    file_name=p.name,
                    mime="image/png" if p.suffix.lower() == ".png" else "application/octet-stream",
                )
                st.divider()

        # Render JSON treemap if exists
        if jsons:
            st.subheader("🧩 JSON Artifacts (Treemap, etc.)")
            for p in jsons:
                st.markdown(f"**{p.name}**")

                try:
                    obj = json.loads(p.read_text(encoding="utf-8"))
                except Exception as e:
                    st.error(f"Không đọc được JSON: {e}")
                    continue

                # Nếu JSON là dạng Plotly figure dict -> render trực tiếp
                if isinstance(obj, dict) and ("data" in obj and "layout" in obj):
                    try:
                        import plotly.graph_objects as go
                        fig = go.Figure(obj)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception:
                        st.json(obj)
                else:
                    st.json(obj)

                st.download_button(
                    f"⬇️ Download {p.name}",
                    data=p.read_bytes(),
                    file_name=p.name,
                    mime="application/json",
                )
                st.divider()
