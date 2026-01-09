# dashboard/app.py
import os
import glob
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px


# =========================
# Page config
# =========================
st.set_page_config(
    page_title="Used Car Price Analytics Dashboard",
    page_icon="🚗",
    layout="wide",
)

st.title("🚗 Used Car Price Analytics Dashboard")
st.caption("Nguồn dữ liệu: master dataset (gộp từ các nguồn như bonbanh, chotot, otocomvn...)")

# =========================
# Helpers
# =========================
DATA_DIR_DEFAULT = "data/master"
MASTER_GLOB = "master_dataset_all_*.csv"


def find_latest_master_csv(data_dir: str) -> str | None:
    pattern = os.path.join(data_dir, MASTER_GLOB)
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def humanize_int(x):
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return str(x)


def to_numeric_safe(s):
    # chuyển về numeric, lỗi => NaN
    return pd.to_numeric(s, errors="coerce")


def clamp_iqr(series: pd.Series):
    # clamp outlier theo IQR để chart đỡ bị "dính trần"
    s = series.dropna()
    if s.empty:
        return series
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    return series.clip(lower=lo, upper=hi)


@st.cache_data(show_spinner=False)
def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    return df


def standardize_master(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kỳ vọng cột: brand, model, year, price, mileage, fuel, location, color, source, crawl_date
    Nhưng vẫn cố gắng "chịu đựng" nếu thiếu cột / kiểu dữ liệu lạ.
    """
    df = df.copy()

    # đảm bảo các cột tồn tại
    for col in ["brand", "model", "year", "price", "mileage", "fuel", "location", "color", "source", "crawl_date"]:
        if col not in df.columns:
            df[col] = np.nan

    # chuẩn hoá kiểu
    df["brand"] = df["brand"].astype(str).replace({"nan": np.nan}).fillna("Unknown")
    df["model"] = df["model"].astype(str).replace({"nan": np.nan}).fillna("Unknown")
    df["fuel"] = df["fuel"].astype(str).replace({"nan": np.nan}).fillna("Unknown")
    df["location"] = df["location"].astype(str).replace({"nan": np.nan}).fillna("Unknown")
    df["color"] = df["color"].astype(str).replace({"nan": np.nan}).fillna("Unknown")
    df["source"] = df["source"].astype(str).replace({"nan": np.nan}).fillna("Unknown")

    df["year"] = to_numeric_safe(df["year"]).astype("Int64")
    df["price"] = to_numeric_safe(df["price"])
    df["mileage"] = to_numeric_safe(df["mileage"])

    # crawl_date -> datetime (coerce)
    df["crawl_date"] = pd.to_datetime(df["crawl_date"], errors="coerce")

    # loại row cực bẩn
    df = df.dropna(subset=["price", "year"], how="any")
<<<<<<< HEAD

    # price/mileage âm -> NaN
=======
# price/mileage âm -> NaN
>>>>>>> daca89c9e2d6901ba83017287808cf9dcda97f35
    df.loc[df["price"] < 0, "price"] = np.nan
    df.loc[df["mileage"] < 0, "mileage"] = np.nan

    df = df.dropna(subset=["price", "year"], how="any")

    return df


def apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    out = df.copy()

    if f["sources"]:
        out = out[out["source"].isin(f["sources"])]

    if f["brands"]:
        out = out[out["brand"].isin(f["brands"])]

    if f["fuels"]:
        out = out[out["fuel"].isin(f["fuels"])]

    if f["locations"]:
        out = out[out["location"].isin(f["locations"])]

    if f["colors"]:
        out = out[out["color"].isin(f["colors"])]

    # year range
    y_min, y_max = f["year_range"]
    out = out[(out["year"].fillna(0) >= y_min) & (out["year"].fillna(0) <= y_max)]

    # price range (VNĐ)
    p_min, p_max = f["price_range"]
    out = out[(out["price"] >= p_min) & (out["price"] <= p_max)]

    # mileage range
    m_min, m_max = f["mileage_range"]
    if "mileage" in out.columns:
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


# =========================
# Sidebar - Data source
# =========================
with st.sidebar:
    st.header("⚙️ Cấu hình")

    st.subheader("📁 Dataset")
    data_dir = st.text_input("Thư mục master dataset", value=DATA_DIR_DEFAULT)
    latest = find_latest_master_csv(data_dir)

    uploaded = st.file_uploader("Hoặc upload CSV (master_dataset...)", type=["csv"])

    if uploaded is None:
        if latest:
            st.success(f"Auto chọn file mới nhất:\n{latest}")
        else:
            st.warning(f"Không tìm thấy file {MASTER_GLOB} trong {data_dir}")

    st.divider()
    st.subheader("🎛️ Lọc dữ liệu")
    # Filter UI sẽ render sau khi load df


# =========================
# Load data
# =========================
df_raw = None
data_label = ""

try:
    if uploaded is not None:
        df_raw = pd.read_csv(uploaded)
        data_label = f"Uploaded: {uploaded.name}"
    else:
        if latest is None:
            st.error("Không có dataset để load. Hãy kiểm tra thư mục hoặc upload file CSV.")
            st.stop()
        df_raw = load_csv(latest)
        data_label = f"File: {os.path.basename(latest)}"
except Exception as e:
    st.error(f"Lỗi load dataset: {e}")
    st.stop()

df = standardize_master(df_raw)

st.caption(f"✅ Đã load: **{data_label}** | Rows sau chuẩn hoá: **{len(df):,}**".replace(",", "."))

# =========================
# Sidebar filters (need df)
# =========================
with st.sidebar:
    # Prepare options
    sources_all = sorted(df["source"].dropna().unique().tolist())
    brands_all = sorted(df["brand"].dropna().unique().tolist())
    fuels_all = sorted(df["fuel"].dropna().unique().tolist())
    locations_all = sorted(df["location"].dropna().unique().tolist())
    colors_all = sorted(df["color"].dropna().unique().tolist())

    # ranges
    year_min = int(df["year"].min()) if df["year"].notna().any() else 2000
    year_max = int(df["year"].max()) if df["year"].notna().any() else 2026

    price_min = float(df["price"].min()) if df["price"].notna().any() else 0
    price_max = float(df["price"].max()) if df["price"].notna().any() else 1_000_000_000

    mileage_min = float(df["mileage"].min()) if df["mileage"].notna().any() else 0
    mileage_max = float(df["mileage"].max()) if df["mileage"].notna().any() else 300_000

    # widgets
    selected_sources = st.multiselect("Source", sources_all, default=sources_all)
    selected_brands = st.multiselect("Brand", brands_all, default=[])
    selected_fuels = st.multiselect("Fuel", fuels_all, default=[])
    selected_locations = st.multiselect("Location", locations_all, default=[])
    selected_colors = st.multiselect("Color", colors_all, default=[])

    year_range = st.slider("Year range", min_value=year_min, max_value=year_max, value=(year_min, year_max))
    price_range = st.slider(
        "Price range (VNĐ)",
        min_value=float(price_min),
        max_value=float(price_max),
        value=(float(price_min), float(price_max)),
        step=max(1.0, float((price_max - price_min) / 200) if price_max > price_min else 1.0),
        format="%.0f",
    )
    mileage_range = st.slider(
        "Mileage range (km)",
        min_value=float(mileage_min),
        max_value=float(mileage_max),
        value=(float(mileage_min), float(mileage_max)),
        step=max(1.0, float((mileage_max - mileage_min) / 200) if mileage_max > mileage_min else 1.0),
        format="%.0f",
    )

    search_text = st.text_input("Search (brand/model/location)", value="")

    clamp_outliers = st.checkbox("Clamp outliers (IQR) cho chart giá/km", value=True)
    top_n = st.slider("Top N (brand/model/location)", 5, 30, 10)

<<<<<<< HEAD
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

# =========================
# KPIs
# =========================
left, mid, right, r2 = st.columns(4)
with left:
    st.metric("Số tin", f"{len(df_f):,}".replace(",", "."))
with mid:
    st.metric("Số hãng", f"{df_f['brand'].nunique():,}".replace(",", "."))
with right:
    st.metric("Giá trung vị", f"{humanize_int(df_f['price'].median())} VNĐ")
with r2:
    st.metric("Giá trung bình", f"{humanize_int(df_f['price'].mean())} VNĐ")

st.divider()

# Optionally clamp outliers for visualization only
price_vis = df_f["price"].copy()
mileage_vis = df_f["mileage"].copy()
if clamp_outliers:
    price_vis = clamp_iqr(price_vis)
    mileage_vis = clamp_iqr(mileage_vis)

df_vis = df_f.copy()
df_vis["price_vis"] = price_vis
df_vis["mileage_vis"] = mileage_vis

# =========================
# Charts row 1
# =========================
c1, c2 = st.columns(2)

with c1:
    st.subheader("📈 Phân phối giá (VNĐ)")
    fig = px.histogram(
        df_vis,
        x="price_vis",
        nbins=60,
        title="Price Distribution (clamped)" if clamp_outliers else "Price Distribution",
    )
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("📅 Số tin theo năm sản xuất")
    year_counts = (
        df_f.dropna(subset=["year"])
        .groupby("year", dropna=True)
        .size()
        .reset_index(name="count")
        .sort_values("year")
    )
    fig = px.bar(year_counts, x="year", y="count", title="Listings by Year")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)

# =========================
# Charts row 2
# =========================
c3, c4 = st.columns(2)

with c3:
    st.subheader("🏷️ Top hãng xe")
    top_brand = (
        df_f.groupby("brand")
        .size()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index(name="count")
    )
    fig = px.bar(top_brand, x="brand", y="count", title=f"Top {top_n} Brands")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("🌍 Top khu vực")
    top_loc = (
        df_f.groupby("location")
        .size()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index(name="count")
    )
    fig = px.bar(top_loc, x="location", y="count", title=f"Top {top_n} Locations")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)

# =========================
# Charts row 3
# =========================
c5, c6 = st.columns(2)

with c5:
    st.subheader("⛽ Fuel breakdown")
    fuel_counts = (
        df_f.groupby("fuel")
        .size()
        .sort_values(ascending=False)
        .reset_index(name="count")
    )
    fig = px.pie(fuel_counts, names="fuel", values="count", title="Fuel Type Share")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)

with c6:
    st.subheader("🔗 Giá vs Số km")
    # scatter có thể nặng, sample nếu quá lớn
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
=======
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

    # =========================
    # KPIs
    # =========================
    left, mid, right, r2 = st.columns(4)
    with left:
        st.metric("Số tin", f"{len(df_f):,}".replace(",", "."))
    with mid:
        st.metric("Số hãng", f"{df_f['brand'].nunique():,}".replace(",", "."))
    with right:
        st.metric("Giá trung vị", f"{humanize_int(df_f['price'].median())} VNĐ")
    with r2:
        st.metric("Giá trung bình", f"{humanize_int(df_f['price'].mean())} VNĐ")
    st.divider()

    # Optionally clamp outliers for visualization only
    price_vis = df_f["price"].copy()
    mileage_vis = df_f["mileage"].copy()
    if clamp_outliers:
        price_vis = clamp_iqr(price_vis)
        mileage_vis = clamp_iqr(mileage_vis)

    df_vis = df_f.copy()
    df_vis["price_vis"] = price_vis
    df_vis["mileage_vis"] = mileage_vis

    # =========================
    # Charts row 1
    # =========================
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("📈 Phân phối giá (VNĐ)")
        fig = px.histogram(
            df_vis,
            x="price_vis",
            nbins=60,
            title="Price Distribution (clamped)" if clamp_outliers else "Price Distribution",
        )
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("📅 Số tin theo năm sản xuất")
        year_counts = (
            df_f.dropna(subset=["year"])
            .groupby("year", dropna=True)
            .size()
            .reset_index(name="count")
            .sort_values("year")
        )
        fig = px.bar(year_counts, x="year", y="count", title="Listings by Year")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # Charts row 2
    # =========================
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("🏷️ Top hãng xe")
        top_brand = (
            df_f.groupby("brand")
            .size()
            .sort_values(ascending=False)
            .head(top_n)
            .reset_index(name="count")
        )
        fig = px.bar(top_brand, x="brand", y="count", title=f"Top {top_n} Brands")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        st.subheader("🌍 Top khu vực")
        top_loc = (
            df_f.groupby("location")
            .size()
            .sort_values(ascending=False)
            .head(top_n)
            .reset_index(name="count")
        )
        fig = px.bar(top_loc, x="location", y="count", title=f"Top {top_n} Locations")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # Charts row 3
    # =========================
    c5, c6 = st.columns(2)

    with c5:
        st.subheader("⛽ Fuel breakdown")
        fuel_counts = (
            df_f.groupby("fuel")
            .size()
            .sort_values(ascending=False)
            .reset_index(name="count")
        )
        fig = px.pie(fuel_counts, names="fuel", values="count", title="Fuel Type Share")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c6:
        st.subheader("🔗 Giá vs Số km")
        # scatter có thể nặng, sample nếu quá lớn
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
    # Charts row 4 - Brand price boxplot
    # =========================
    st.subheader("📦 Phân phối giá theo hãng (Top)")
    top_brand_names = top_brand["brand"].tolist()
    box_df = df_vis[df_vis["brand"].isin(top_brand_names)].copy()
    if len(box_df) > 20000:
        box_df = box_df.sample(20000, random_state=42)

    fig = px.box(
        box_df,
        x="brand",
        y="price_vis",
        points="outliers",
        title="Price by Brand (Top brands)",
>>>>>>> daca89c9e2d6901ba83017287808cf9dcda97f35
    )
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)

<<<<<<< HEAD
# =========================
# Charts row 4 - Brand price boxplot
# =========================
st.subheader("📦 Phân phối giá theo hãng (Top)")
top_brand_names = top_brand["brand"].tolist()
box_df = df_vis[df_vis["brand"].isin(top_brand_names)].copy()
if len(box_df) > 20000:
    box_df = box_df.sample(20000, random_state=42)

fig = px.box(
    box_df,
    x="brand",
    y="price_vis",
    points="outliers",
    title="Price by Brand (Top brands)",
)
fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
st.plotly_chart(fig, use_container_width=True)

# =========================
# Crawl timeline (if crawl_date exists)
# =========================
st.subheader("🗓️ Số tin theo ngày crawl")
if df_f["crawl_date"].notna().any():
    daily = (
        df_f.dropna(subset=["crawl_date"])
        .assign(day=lambda d: d["crawl_date"].dt.date)
        .groupby(["day", "source"])
        .size()
        .reset_index(name="count")
        .sort_values("day")
    )
    fig = px.line(daily, x="day", y="count", color="source", markers=True, title="Crawled listings per day")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Dataset không có crawl_date hợp lệ để vẽ timeline.")

# =========================
# Data preview
# =========================
st.subheader("🔍 Xem dữ liệu (preview)")
show_cols = ["brand", "model", "year", "price", "mileage", "fuel", "location", "color", "source", "crawl_date"]
show_cols = [c for c in show_cols if c in df_f.columns]

st.dataframe(
    df_f[show_cols].sort_values(by=["year", "price"], ascending=[False, False]).head(200),
    use_container_width=True,
    height=420,
)

st.caption("Tip: Nếu chart bị chậm, hãy lọc bớt Brand/Location hoặc bật clamp outliers, và giảm Top N.")
=======
    # =========================
    # Crawl timeline (if crawl_date exists)
    # =========================
    st.subheader("🗓️ Số tin theo ngày crawl")
    if df_f["crawl_date"].notna().any():
        daily = (
            df_f.dropna(subset=["crawl_date"])
            .assign(day=lambda d: d["crawl_date"].dt.date)
            .groupby(["day", "source"])
            .size()
            .reset_index(name="count")
            .sort_values("day")
        )
        fig = px.line(daily, x="day", y="count", color="source", markers=True, title="Crawled listings per day")
        fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Dataset không có crawl_date hợp lệ để vẽ timeline.")

    # =========================
    # Data preview
    # =========================
    st.subheader("🔍 Xem dữ liệu (preview)")
    show_cols = ["brand", "model", "year", "price", "mileage", "fuel", "location", "color", "source", "crawl_date"]
    show_cols = [c for c in show_cols if c in df_f.columns]

    st.dataframe(
        df_f[show_cols].sort_values(by=["year", "price"], ascending=[False, False]).head(200),
        use_container_width=True,
        height=420,
    )

    st.caption("Tip: Nếu chart bị chậm, hãy lọc bớt Brand/Location hoặc bật clamp outliers, và giảm Top N.")
>>>>>>> daca89c9e2d6901ba83017287808cf9dcda97f35
