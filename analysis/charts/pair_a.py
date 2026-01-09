

from __future__ import annotations

from typing import Dict, List
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis import config, utils


def clamp_iqr(series: pd.Series) -> pd.Series:
    s = series.dropna()
    if s.empty:
        return series
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    return series.clip(lower=lo, upper=hi)


def prepare_for_visualization(
    df: pd.DataFrame,
    clamp_outliers: bool = False,
) -> pd.DataFrame:
    """
    Return a copy with helper columns for visualization.
    - price_vis, mileage_vis are optionally clamped to reduce skew.
    """

    df_vis = df.copy()
    df_vis["price_vis"] = clamp_iqr(df["price"]) if clamp_outliers else df["price"]
    if "mileage" in df.columns:
        df_vis["mileage_vis"] = clamp_iqr(df["mileage"]) if clamp_outliers else df["mileage"]
    else:
        df_vis["mileage_vis"] = pd.NA
    return df_vis


def price_distribution(df_vis: pd.DataFrame, nbins: int = 60, clamp_outliers: bool = False):
    title = "Price Distribution (clamped)" if clamp_outliers else "Price Distribution"
    fig = px.histogram(df_vis, x="price_vis", nbins=nbins, title=title)
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    return fig


def listings_by_year(df: pd.DataFrame):
    year_counts = (
        df.dropna(subset=["year"])
        .groupby("year", dropna=True)
        .size()
        .reset_index(name="count")
        .sort_values("year")
    )
    fig = px.bar(year_counts, x="year", y="count", title="Listings by Year")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    return fig


def top_brands(df: pd.DataFrame, top_n: int = 10):
    top_brand = (
        df.groupby("brand")
        .size()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index(name="count")
    )
    fig = px.bar(top_brand, x="brand", y="count", title=f"Top {top_n} Brands")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    return fig, top_brand["brand"].tolist()


def top_locations(df: pd.DataFrame, top_n: int = 10):
    top_loc = (
        df.groupby("location")
        .size()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index(name="count")
    )
    fig = px.bar(top_loc, x="location", y="count", title=f"Top {top_n} Locations")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    return fig


def fuel_share(df: pd.DataFrame):
    fuel_counts = (
        df.groupby("fuel")
        .size()
        .sort_values(ascending=False)
        .reset_index(name="count")
    )
    fig = px.pie(fuel_counts, names="fuel", values="count", title="Fuel Type Share")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    return fig


def price_vs_mileage(
    df_vis: pd.DataFrame,
    sample_size: int = 15000,
):
    plot_df = df_vis.copy()
    if len(plot_df) > sample_size:
        plot_df = plot_df.sample(sample_size, random_state=42)

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
    return fig


def price_by_brand_box(
    df_vis: pd.DataFrame,
    top_brands: List[str],
    sample_size: int = 20000,
):
    box_df = df_vis[df_vis["brand"].isin(top_brands)].copy()
    if len(box_df) > sample_size:
        box_df = box_df.sample(sample_size, random_state=42)

    fig = px.box(
        box_df,
        x="brand",
        y="price_vis",
        points="outliers",
        title="Price by Brand (Top brands)",
        color="brand",
        color_discrete_map=config.BRAND_COLORS,
    )
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    return fig


def crawl_timeline(df: pd.DataFrame):
    if "crawl_date" not in df.columns or not df["crawl_date"].notna().any():
        return None

    daily = (
        df.dropna(subset=["crawl_date"])
        .assign(day=lambda d: d["crawl_date"].dt.date)
        .groupby(["day", "source"])
        .size()
        .reset_index(name="count")
        .sort_values("day")
    )
    fig = px.line(daily, x="day", y="count", color="source", markers=True, title="Crawled listings per day")
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    return fig


def kpis(df: pd.DataFrame) -> Dict[str, int]:
    return {
        "num_listings": int(len(df)),
        "num_brands": int(df["brand"].nunique()),
        "median_price": float(df["price"].median()) if not df["price"].empty else 0,
        "mean_price": float(df["price"].mean()) if not df["price"].empty else 0,
    }


def build_dashboard_charts(
    df: pd.DataFrame,
    *,
    clamp_outliers: bool = False,
    top_n: int = 10,
    scatter_sample: int = 15000,
    box_sample: int = 20000,
):
    """Create all charts used in the Streamlit app and return them as a dict."""

    df_vis = prepare_for_visualization(df, clamp_outliers=clamp_outliers)

    fig_price_dist = price_distribution(df_vis, clamp_outliers=clamp_outliers)
    fig_year_counts = listings_by_year(df)
    fig_top_brand, top_brand_names = top_brands(df, top_n=top_n)
    fig_top_loc = top_locations(df, top_n=top_n)
    fig_fuel = fuel_share(df)
    fig_scatter = price_vs_mileage(df_vis, sample_size=scatter_sample)
    fig_box = price_by_brand_box(df_vis, top_brand_names, sample_size=box_sample)
    fig_timeline = crawl_timeline(df)

    return {
        "kpis": kpis(df),
        "price_distribution": fig_price_dist,
        "listings_by_year": fig_year_counts,
        "top_brands": fig_top_brand,
        "top_brand_names": top_brand_names,
        "top_locations": fig_top_loc,
        "fuel_share": fig_fuel,
        "price_vs_mileage": fig_scatter,
        "price_by_brand_box": fig_box,
        "crawl_timeline": fig_timeline,
        "df_vis": df_vis,
    }


def save_all_interactive(charts: Dict[str, object]):
    """
    Convenience helper to persist Plotly figures for debugging/offline use.
    Only saves interactive figures; skip None objects.
    """

    for name, fig in charts.items():
        if fig is None:
            continue
        # Only save Plotly Figure-like objects (have write_json). Skip dataframes/metadata.
        if hasattr(fig, "write_json"):
            utils.save_interactive_plot(fig, f"pair_a_{name}.json")


if __name__ == "__main__":
    print("\n [PAIR A] Bắt đầu tạo charts...")
    
    
    df = utils.load_master_data()
    
    if df is None:
        print(" Không tìm thấy dữ liệu master. Chạy preprocessing trước!")
        exit(1)
    
    print(f" Đã load {len(df):,} dòng dữ liệu".replace(",", "."))
    
    
    charts = build_dashboard_charts(
        df,
        clamp_outliers=True,
        top_n=10,
    )
    
    
    kpi = charts["kpis"]
    print(f"\n    python -m analysis.charts.pair_a    python -m analysis.charts.pair_a KPIs:")
    print(f"   - Số tin: {kpi['num_listings']:,}".replace(",", "."))
    print(f"   - Số hãng: {kpi['num_brands']}")
    print(f"   - Giá trung vị: {kpi['median_price']:,.0f} VNĐ".replace(",", "."))
    print(f"   - Giá TB: {kpi['mean_price']:,.0f} VNĐ".replace(",", "."))
    
    
    print(f"\n Đang lưu charts...")
    save_all_interactive(charts)
    
    print(f"\n Hoàn thành! Xem charts tại: {config.OUTPUT_DIR}")
    print(f"   Các file JSON có thể mở bằng Plotly hoặc Streamlit.")