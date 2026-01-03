import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# ==========================================
# 1. CẤU HÌNH NGƯỜI DÙNG
# ==========================================
# 👇 Điền tên file cụ thể nếu muốn chạy riêng lẻ (VD: "chotot_clean_20251223.csv")
# 👇 Để TRỐNG ("") để chạy chế độ BATCH (xử lý tất cả file chưa processed).
SPECIFIC_FILENAME = "" 

# ==========================================
# 2. CẤU HÌNH LOGIC XỬ LÝ (Business Rules)
# ==========================================
MIN_PRICE = 10_000_000       # 10 Triệu (Bỏ xác xe/xe đồ chơi)
MAX_PRICE = 100_000_000_000  # 100 Tỷ (Tránh số liệu ảo)
CURRENT_YEAR = datetime.now().year

def get_repo_root() -> Path:
    """Lùi 2 cấp: preprocessing -> root"""
    return Path(__file__).resolve().parent.parent

def setup_paths():
    repo_root = get_repo_root()
    clean_dir = repo_root / "data" / "clean"
    processed_dir = repo_root / "data" / "processed"
    
    # Tạo thư mục đích nếu chưa có
    processed_dir.mkdir(parents=True, exist_ok=True)
    return clean_dir, processed_dir

def transform_logic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Core Logic: Làm sạch sâu, tính toán cột mới, lọc rác
    """
    # 1. Chuyển đổi kiểu dữ liệu
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['mileage_v2'] = pd.to_numeric(df['mileage_v2'], errors='coerce')
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['crawled_at'] = pd.to_datetime(df['crawled_at'], errors='coerce')

    # 2. Xử lý Missing Value
    # Xóa dòng nếu mất Giá hoặc Năm (thông tin quan trọng nhất)
    df = df.dropna(subset=['price', 'year'])
    
    # Điền khuyết cho các cột phân loại
    df['carcolor_name'] = df['carcolor_name'].fillna('Unknown')
    df['fuel'] = df['fuel'].fillna('Other')
    df['region_name'] = df['region_name'].fillna('Unknown')
    df['brand'] = df['brand'].fillna('Unknown')
    df['model'] = df['model'].fillna('Unknown')

    # 3. Feature Engineering (Tạo cột mới)
    # Tuổi xe (Car Age)
    df['car_age'] = CURRENT_YEAR - df['year']
    
    # Ghép Hãng + Dòng xe (VD: "Toyota Vios")
    df['brand_model'] = df['brand'].astype(str) + ' ' + df['model'].astype(str)

    # 4. Lọc dữ liệu rác (Filtering)
    # Lọc giá hợp lý
    df = df[ (df['price'] >= MIN_PRICE) & (df['price'] <= MAX_PRICE) ]
    
    # Lọc năm sản xuất (1980 -> Năm sau)
    df = df[ (df['year'] >= 1980) & (df['year'] <= CURRENT_YEAR + 1) ]
    
    # Lọc tuổi xe (không âm)
    df = df[ df['car_age'] >= 0 ]

    return df

def process_file(input_path: Path, output_path: Path):
    """Đọc file clean -> transform -> lưu file processed"""
    try:
        # Đọc CSV
        df = pd.read_csv(input_path)
        
        if df.empty:
            print(f"   ⚠️  File rỗng, bỏ qua: {input_path.name}")
            return

        # Thực hiện biến đổi
        original_rows = len(df)
        df_processed = transform_logic(df)
        remaining_rows = len(df_processed)
        
        # Lưu file
        df_processed.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"   ✅ Đã tạo: {output_path.name}")
        print(f"      (Giữ lại {remaining_rows}/{original_rows} dòng hợp lệ)")
        
    except Exception as e:
        print(f"   ❌ Lỗi xử lý file {input_path.name}: {e}")

def run_batch_all():
    """Quét và xử lý tất cả file trong data/clean chưa có trong data/processed"""
    clean_dir, processed_dir = setup_paths()
    
    if not clean_dir.exists():
        print(f"❌ Thư mục {clean_dir} không tồn tại. Hãy chạy cleaning.py trước.")
        return

    # Lấy danh sách file clean
    all_clean_files = sorted(list(clean_dir.glob("*.csv")))
    
    if not all_clean_files:
        print("❌ Không tìm thấy file CSV nào trong data/clean/")
        return

    print(f"🔍 Tìm thấy {len(all_clean_files)} file clean. Bắt đầu kiểm tra...")
    
    processed_count = 0
    skipped_count = 0

    for input_path in all_clean_files:
        # Tạo tên output: thay 'clean' thành 'processed'
        processed_name = input_path.name.replace("clean", "processed")
        
        # Fallback nếu tên file không chuẩn
        if processed_name == input_path.name:
            processed_name = f"processed_{input_path.name}"
            
        output_path = processed_dir / processed_name

        # LOGIC CHECK TỒN TẠI
        if output_path.exists():
            skipped_count += 1
        else:
            print(f"🚀 Đang biến đổi: {input_path.name} ...")
            process_file(input_path, output_path)
            processed_count += 1

    print("-" * 30)
    print(f"🎉 HOÀN TẤT TRANSFORMATION!")
    print(f"   - Đã xử lý mới: {processed_count} file")
    print(f"   - Đã bỏ qua (cũ): {skipped_count} file")

# ==========================================
# 3. KHU VỰC CHẠY CHƯƠNG TRÌNH
# ==========================================
if __name__ == "__main__":
    clean_dir, processed_dir = setup_paths()

    # ƯU TIÊN 1: Chạy file cụ thể (Hard Code)
    if SPECIFIC_FILENAME and SPECIFIC_FILENAME.strip():
        print(f"🎯 CHẾ ĐỘ HARD CODE: Chỉ xử lý {SPECIFIC_FILENAME}")
        input_path = clean_dir / SPECIFIC_FILENAME
        
        if input_path.exists():
            output_name = input_path.name.replace("clean", "processed")
            process_file(input_path, processed_dir / output_name)
        else:
            print(f"❌ Không tìm thấy file {SPECIFIC_FILENAME} trong {clean_dir}")

    # ƯU TIÊN 2: Chạy Batch (Mặc định)
    else:
        run_batch_all()