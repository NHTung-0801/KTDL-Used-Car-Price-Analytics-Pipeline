import pandas as pd
import glob
import os
from datetime import datetime

def aggregate_master_data():
    """
    Gộp tất cả file sạch từ data/cleaned vào thư mục data/master
    """
    CLEANED_FOLDER = 'data/cleaned'
    MASTER_FOLDER = 'data/master'
    
    # 1. Tự động tạo thư mục master nếu chưa có
    os.makedirs(MASTER_FOLDER, exist_ok=True)
    
    # 2. Tìm tất cả các file đã làm sạch (có hậu tố _cleaned_)
    all_cleaned_files = glob.glob(os.path.join(CLEANED_FOLDER, "*_cleaned_*.csv"))
    
    if not all_cleaned_files:
        print(f"⚠️ Không tìm thấy dữ liệu sạch nào trong {CLEANED_FOLDER}!")
        return

    print(f"🔄 Bắt đầu tổng hợp {len(all_cleaned_files)} nguồn dữ liệu...")
    
    list_df = []
    for filename in all_cleaned_files:
        try:
            df = pd.read_csv(filename, encoding='utf-8-sig')
            list_df.append(df)
            print(f"   -> Đã nạp: {os.path.basename(filename)}")
        except Exception as e:
            print(f"   ❌ Lỗi khi đọc {filename}: {e}")

    if list_df:
        # 3. Gộp tất cả DataFrames
        master_df = pd.concat(list_df, ignore_index=True)
        
        # 4. Loại bỏ trùng lặp (Deduplication)
        # Tránh trường hợp một xe đăng trên nhiều trang web bị tính trùng
        before_count = len(master_df)
        master_df.drop_duplicates(subset=['brand', 'model', 'year', 'price', 'mileage'], inplace=True)
        after_count = len(master_df)
        
        # 5. Đặt tên file có timestamp để phân biệt các lần tổng hợp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_filename = f"master_dataset_all_{timestamp}.csv"
        output_path = os.path.join(MASTER_FOLDER, output_filename)
        
        # 6. Lưu file vào mục master
        master_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*50)
        print(f"✅ TỔNG HỢP THÀNH CÔNG!")
        print(f"📊 Số lượng ban đầu: {before_count}")
        print(f"🧹 Số lượng sau khi lọc trùng: {after_count}")
        print(f"💾 File lưu tại: {output_path}")
        print("="*50)
    else:
        print("❌ Không có dữ liệu hợp lệ để tổng hợp.")

if __name__ == "__main__":
    aggregate_master_data()