import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter

from analysis import utils, config


def format_price_vn(x, pos=None):
    """Format giá theo kiểu VN: Tr / Tỷ"""
    try:
        x = float(x)
    except Exception:
        return str(x)

    if x >= 1e9:
        return f"{x/1e9:.1f} Tỷ"
    if x >= 1e6:
        return f"{x/1e6:.0f} Tr"
    return f"{x:.0f}"


def run_analysis(df):
    print("\n--- [CẶP C] Đang xử lý Phân tích Nâng cao... ---")

    # =========================================================
    # Chuẩn hoá / lọc dữ liệu cơ bản
    # =========================================================
    required_cols = {"price", "year", "brand"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"[pair_c] Thiếu cột bắt buộc: {missing}")

    df = df.copy()
    df["price"] = df["price"].astype("float64", errors="ignore")
    df["year"] = df["year"].astype("float64", errors="ignore")

    # Lọc rows rác
    df = df.dropna(subset=["price", "year", "brand"])
    df = df[df["price"] > 0]
    df["year"] = df["year"].astype(int, errors="ignore")

    # =========================================================
    # 1) Scatter: Tương quan Năm vs Giá
    # =========================================================
    print("   -> Đang tạo Scatter Plot (Năm vs Giá)...")

    plt.figure(figsize=(12, 7))

    # Lọc nhiễu (tuỳ dataset có thể chỉnh)
    df_plot = df[(df["price"] < 10e9) & (df["year"] >= 2005)].copy()

    # Nếu data quá lớn -> sample cho nhẹ máy (vẫn giữ xu hướng)
    max_points = 6000
    if len(df_plot) > max_points:
        df_plot = df_plot.sample(max_points, random_state=42)

    # Group brand: top brands theo config.BRAND_COLORS, còn lại Other
    brand_colors = getattr(config, "BRAND_COLORS", {}) or {}
    known_brands = set(brand_colors.keys())

    df_plot["brand_group"] = df_plot["brand"].apply(lambda x: x if x in known_brands else "Other")

    # đảm bảo có màu cho "Other"
    palette = dict(brand_colors)
    if "Other" not in palette:
        palette["Other"] = "#9e9e9e"  # xám (đỡ nổi)

    # sort để Other xuống cuối legend
    df_plot["is_other"] = (df_plot["brand_group"] == "Other").astype(int)
    df_plot = df_plot.sort_values("is_other").drop(columns=["is_other"])

    ax = sns.scatterplot(
        data=df_plot,
        x="year",
        y="price",
        hue="brand_group",
        alpha=0.7,
        s=55,
        palette=palette
    )

    plt.title("Tương quan Giá xe theo Năm sản xuất", fontsize=14, fontweight="bold")
    plt.xlabel("Năm sản xuất", fontsize=12)
    plt.ylabel("Giá bán", fontsize=12)

    ax.yaxis.set_major_formatter(FuncFormatter(format_price_vn))

    # legend gọn gàng
    leg = ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", borderaxespad=0.0, title="Hãng")
    if leg:
        for txt in leg.get_texts():
            txt.set_fontsize(9)

    plt.tight_layout()
    utils.save_static_plot(plt, "pair_c_correlation.png")
    plt.close()

    # =========================================================
    # 2) Boxplot: Phân bố giá Top 10 hãng (log scale)
    # =========================================================
    print("   -> Đang tạo Boxplot (Phân bố giá)...")

    plt.figure(figsize=(12, 6))

    top_10_brands = df["brand"].value_counts().head(10).index
    df_box = df[df["brand"].isin(top_10_brands)].copy()

    # lọc nhiễu cho boxplot (đỡ bị kéo quá xa)
    df_box = df_box[(df_box["price"] > 0) & (df_box["price"] < 10e9)]

    ax = sns.boxplot(
        data=df_box,
        x="brand",
        y="price",
        hue="brand",       # tránh warning seaborn mới khi dùng palette
        palette="Set2",
        dodge=False
    )

    # tắt legend (seaborn boxplot đôi khi tự tạo)
    leg = ax.get_legend()
    if leg is not None:
        leg.remove()

    plt.yscale("log")
    plt.title("Phân bố khoảng giá của Top 10 Hãng xe (Thang đo Log)", fontsize=14)
    plt.ylabel("Giá xe (Log Scale)")
    plt.xlabel("Hãng xe")

    ax.yaxis.set_major_formatter(FuncFormatter(format_price_vn))
    plt.xticks(rotation=25, ha="right")

    plt.tight_layout()
    utils.save_static_plot(plt, "pair_c_price_distribution.png")
    plt.close()

    print("✅ Hoàn thành Cặp C!")
