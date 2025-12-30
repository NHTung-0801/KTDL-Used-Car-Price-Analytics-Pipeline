import pandas as pd
import glob
import os
from datetime import datetime

def aggregate_master_data():
    """
    Gộp tất cả file đã làm sạch từ data/cleaned vào thư mục data/master
    Thực hiện deduplication và đảm bảo schema nhất quán
    """
    CLEANED_FOLDER = 'data/cleaned'
    MASTER_FOLDER = 'data/master'
    
    # Tự động tạo thư mục master nếu chưa có
    os.makedirs(MASTER_FOLDER, exist_ok=True)
    
    # Tìm tất cả các file đã làm sạch (có hậu tố _cleaned_)
    all_cleaned_files = glob.glob(os.path.join(CLEANED_FOLDER, "*_cleaned_*.csv"))
    
    if not all_cleaned_files:
        print(f"⚠️ Không tìm thấy dữ liệu sạch nào trong {CLEANED_FOLDER}!")
        print(f"   Hãy chạy 'python preprocessing/cleaning.py' trước.")
        return
    
    print(f"🔄 Bắt đầu tổng hợp {len(all_cleaned_files)} file dữ liệu đã làm sạch...")
    
    list_df = []
    for filename in all_cleaned_files:
        try:
            df = pd.read_csv(filename, encoding='utf-8-sig')
            
            # Kiểm tra schema
            required_cols = ['brand', 'model', 'year', 'price', 'mileage', 'fuel', 
                           'location', 'color', 'source', 'crawl_date']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"   ⚠️ File {os.path.basename(filename)} thiếu cột: {missing_cols}")
                continue
            
            # Đảm bảo kiểu dữ liệu đúng
            df['price'] = pd.to_numeric(df['price'], errors='coerce').astype('Int64')
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
            df['mileage'] = pd.to_numeric(df['mileage'], errors='coerce').astype('Int64')
            
            # Loại bỏ các dòng có giá trị null ở các trường quan trọng
            df = df.dropna(subset=['price', 'year'])
            
            list_df.append(df)
            print(f"   ✅ Đã nạp: {os.path.basename(filename)} ({len(df):,} dòng)")
            
        except Exception as e:
            print(f"   ❌ Lỗi khi đọc {filename}: {e}")
    
    if not list_df:
        print("❌ Không có dữ liệu hợp lệ để tổng hợp.")
        return
    
    # Gộp tất cả DataFrames
    print(f"\n📊 Đang gộp {len(list_df)} file...")
    master_df = pd.concat(list_df, ignore_index=True)
    
    print(f"   -> Tổng số dòng trước khi deduplication: {len(master_df):,}")
    
    # Loại bỏ trùng lặp (Deduplication)
    # Tránh trường hợp một xe đăng trên nhiều trang web bị tính trùng
    # Sử dụng brand, model, year, price, mileage để xác định trùng lặp
    before_count = len(master_df)
    
    # Làm sạch dữ liệu trước khi deduplication
    master_df['brand'] = master_df['brand'].str.strip()
    master_df['model'] = master_df['model'].str.strip()
    
    # Deduplication: loại bỏ các dòng trùng lặp
    # Giữ lại dòng đầu tiên khi có trùng
    master_df = master_df.drop_duplicates(
        subset=['brand', 'model', 'year', 'price', 'mileage'],
        keep='first'
    )
    
    after_count = len(master_df)
    removed_duplicates = before_count - after_count
    
    # Sắp xếp theo năm và giá (để dễ xem)
    master_df = master_df.sort_values(['year', 'price'], ascending=[False, True])
    
    # Reset index
    master_df = master_df.reset_index(drop=True)
    
    # Đảm bảo các cột theo đúng thứ tự schema
    schema_cols = ['brand', 'model', 'year', 'price', 'mileage', 'fuel', 
                  'location', 'color', 'source', 'crawl_date']
    master_df = master_df[schema_cols]
    
    # Đặt tên file có timestamp để phân biệt các lần tổng hợp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_filename = f"master_dataset_all_{timestamp}.csv"
    output_path = os.path.join(MASTER_FOLDER, output_filename)
    
    # Lưu file vào mục master
    master_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    # --- THỐNG KÊ ---
    print("\n" + "="*60)
    print(f"✅ TỔNG HỢP THÀNH CÔNG!")
    print(f"📊 Số lượng ban đầu: {before_count:,} dòng")
    print(f"🧹 Đã loại bỏ trùng lặp: {removed_duplicates:,} dòng")
    print(f"📊 Số lượng sau khi deduplication: {after_count:,} dòng")
    print(f"💾 File lưu tại: {output_path}")
    print("="*60)
    
    # Thống kê theo source
    print("\n--- THỐNG KÊ THEO NGUỒN DỮ LIỆU ---")
    source_stats = master_df['source'].value_counts()
    print(source_stats.to_string())
    
    # Thống kê theo brand (Top 10)
    print("\n--- THỐNG KÊ THEO HÃNG XE (Top 10) ---")
    brand_stats = master_df['brand'].value_counts().head(10)
    print(brand_stats.to_string())
    
    # Thống kê theo năm
    print("\n--- THỐNG KÊ THEO NĂM SẢN XUẤT ---")
    year_stats = master_df['year'].value_counts().sort_index(ascending=False).head(10)
    print(year_stats.to_string())
    
    # Thống kê giá
    print("\n--- THỐNG KÊ GIÁ XE ---")
    print(f"   Giá thấp nhất: {master_df['price'].min():,} VNĐ")
    print(f"   Giá cao nhất: {master_df['price'].max():,} VNĐ")
    print(f"   Giá trung bình: {master_df['price'].mean():,.0f} VNĐ")
    print(f"   Giá trung vị: {master_df['price'].median():,.0f} VNĐ")
    
    # Hiển thị mẫu dữ liệu
    print("\n--- MẪU DỮ LIỆU MASTER (10 dòng đầu) ---")
    print(master_df.head(10).to_string())


if __name__ == "__main__":
    aggregate_master_data()
