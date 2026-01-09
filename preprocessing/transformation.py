import pandas as pd
import glob
import os
from datetime import datetime
from sqlalchemy import create_engine, types

# ======================================================
# CONFIG
# ======================================================
CLEANED_FOLDER = "data/cleaned"
MASTER_FOLDER = "data/master"
DB_NAME = "car_project_db.sqlite"

REQUIRED_COLUMNS = [
    'brand', 'model', 'year', 'price', 'mileage',
    'fuel', 'location', 'color', 'source', 'crawl_date'
]

# ======================================================
# MAIN FUNCTION
# ======================================================
def aggregate_master_data():
    """
    Pipeline tổng hợp dữ liệu cuối:
    - Đọc dữ liệu sạch
    - Validate schema
    - Chuẩn hóa dữ liệu
    - Lọc trùng, lọc rác
    - Tạo ID
    - Lưu CSV + SQLite
    """

    os.makedirs(MASTER_FOLDER, exist_ok=True)

    # --------------------------------------------------
    # 1. LOAD FILE CLEANED
    # --------------------------------------------------
    cleaned_files = glob.glob(os.path.join(CLEANED_FOLDER, "*_cleaned_*.csv"))

    if not cleaned_files:
        print(f"⚠️ Không tìm thấy file cleaned trong {CLEANED_FOLDER}")
        return

    print(f"🔄 Đang nạp {len(cleaned_files)} file dữ liệu sạch...")

    dfs = []
    for file in cleaned_files:
        try:
            df = pd.read_csv(file, encoding="utf-8-sig")
            df.columns = df.columns.str.strip()

            # Validate schema
            missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
            if missing_cols:
                print(f"❌ Bỏ qua {os.path.basename(file)} (thiếu cột {missing_cols})")
                continue

            # Ép kiểu dữ liệu
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df['year'] = pd.to_numeric(df['year'], errors='coerce')
            df['mileage'] = pd.to_numeric(df['mileage'], errors='coerce')

            # Bỏ dòng thiếu dữ liệu quan trọng
            df.dropna(subset=['brand', 'price', 'year'], inplace=True)

            dfs.append(df)
            print(f"   ✅ {os.path.basename(file)}: {len(df):,} dòng")

        except Exception as e:
            print(f"   ❌ Lỗi đọc {file}: {e}")

    if not dfs:
        print("❌ Không có dữ liệu hợp lệ để tổng hợp.")
        return

    # --------------------------------------------------
    # 2. MERGE & DEDUPLICATION
    # --------------------------------------------------
    master_df = pd.concat(dfs, ignore_index=True)

    # Chuẩn hóa text
    for col in ['brand', 'model', 'fuel', 'color', 'location']:
        master_df[col] = master_df[col].astype(str).str.strip().str.title()

    # Lọc trùng
    before = len(master_df)
    master_df.drop_duplicates(
        subset=['brand', 'model', 'year', 'price', 'mileage'],
        keep='first',
        inplace=True
    )
    after = len(master_df)

    print(f"🧹 Lọc trùng: {before:,} → {after:,}")

    # --------------------------------------------------
    # 3. DATA STANDARDIZATION
    # --------------------------------------------------
    # Chuẩn hóa nhiên liệu
    fuel_map = {
        'Xăng': 'Xăng', 'Gasoline': 'Xăng', 'Petrol': 'Xăng',
        'Dầu': 'Dầu', 'Diesel': 'Dầu',
        'Điện': 'Điện', 'Electric': 'Điện', 'Ev': 'Điện',
        'Hybrid': 'Hybrid', 'Lai': 'Hybrid'
    }
    master_df['fuel'] = master_df['fuel'].map(fuel_map).fillna(master_df['fuel'])

    # Chuẩn hóa màu
    def normalize_color(c):
        c = str(c).lower()
        if 'trắng' in c: return 'Trắng'
        if 'đen' in c: return 'Đen'
        if 'đỏ' in c: return 'Đỏ'
        if 'bạc' in c: return 'Bạc'
        if 'xám' in c or 'ghi' in c: return 'Xám'
        if 'xanh' in c: return 'Xanh'
        if 'nâu' in c: return 'Nâu'
        if 'vàng' in c or 'cát' in c: return 'Vàng'
        return 'Khác'

    master_df['color_group'] = master_df['color'].apply(normalize_color)

    # Lọc outlier
    current_year = datetime.now().year
    master_df = master_df[
        (master_df['year'] >= 1990) &
        (master_df['year'] <= current_year + 1) &
        (master_df['price'] >= 20_000_000)
    ]

    # --------------------------------------------------
    # 4. SORT & CREATE ID
    # --------------------------------------------------
    master_df.sort_values(
        by=['year', 'price'],
        ascending=[False, True],
        inplace=True
    )
    master_df.reset_index(drop=True, inplace=True)

    master_df.insert(0, 'id', master_df.index + 1)

    # --------------------------------------------------
    # 5. SELECT FINAL COLUMNS
    # --------------------------------------------------
    final_columns = [
        'id', 'brand', 'model', 'year', 'price', 'mileage',
        'fuel', 'location', 'color', 'color_group',
        'source', 'url', 'crawl_date'
    ]
    final_columns = [c for c in final_columns if c in master_df.columns]
    master_df = master_df[final_columns]

    # --------------------------------------------------
    # 6. SAVE CSV
    # --------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    csv_path = os.path.join(
        MASTER_FOLDER,
        f"master_dataset_final_{timestamp}.csv"
    )
    master_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"💾 Đã lưu CSV: {csv_path}")

    # --------------------------------------------------
    # 7. SAVE SQLITE
    # --------------------------------------------------
    db_path = os.path.join(MASTER_FOLDER, DB_NAME)
    engine = create_engine(f"sqlite:///{os.path.abspath(db_path)}")

    master_df.to_sql(
        'cars',
        con=engine,
        if_exists='replace',
        index=False,
        dtype={
            'id': types.Integer(),
            'price': types.BigInteger(),
            'year': types.Integer(),
            'mileage': types.Integer()
        }
    )

    print(f"🗄️  Đã lưu SQLite DB: {db_path}")

    # --------------------------------------------------
    # 8. SUMMARY
    # --------------------------------------------------
    print("\n" + "=" * 60)
    print("✅ HOÀN TẤT PIPELINE MASTER DATA")
    print(f"📊 Tổng số xe sạch: {len(master_df):,}")
    print("🔋 Phân bố nhiên liệu:")
    print(master_df['fuel'].value_counts())
    print("=" * 60)


# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    aggregate_master_data()
