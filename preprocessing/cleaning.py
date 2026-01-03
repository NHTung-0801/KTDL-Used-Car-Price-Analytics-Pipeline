import csv
import sys
from pathlib import Path

# ==========================================
# 1. CẤU HÌNH NGƯỜI DÙNG
# ==========================================
# 👇 Nếu muốn clean lại 1 file cụ thể bất chấp đã có hay chưa, điền tên vào đây.
# 👇 Nếu để TRỐNG (""), code sẽ chạy chế độ BATCH (quét toàn bộ file chưa clean).
SPECIFIC_FILENAME = "" 

# ==========================================
# 2. CẤU HÌNH CÁC CỘT CẦN LẤY
# ==========================================
TARGET_FIELDS = [
    "listing_id",
    "listing_url",
    "brand",
    "model",
    "year",
    "price",
    "mileage_v2",
    "fuel",
    "carcolor_name", 
    "region_name",
    "crawled_at",
    "source"
]

def get_repo_root() -> Path:
    """Lùi ra 2 cấp để về thư mục gốc dự án"""
    return Path(__file__).resolve().parent.parent

def setup_paths():
    repo_root = get_repo_root()
    raw_dir = repo_root / "data" / "raw"
    clean_dir = repo_root / "data" / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir, clean_dir

def clean_file(raw_file: Path, clean_file: Path):
    """Hàm xử lý logic clean cho 1 file"""
    try:
        with open(raw_file, mode='r', encoding='utf-8', newline='') as f_in, \
             open(clean_file, mode='w', encoding='utf-8', newline='') as f_out:
            
            reader = csv.DictReader(f_in)
            
            # Check cột thiếu
            missing = [f for f in TARGET_FIELDS if f not in reader.fieldnames]
            if missing:
                print(f"   ⚠️  Thiếu cột: {missing}")

            writer = csv.DictWriter(f_out, fieldnames=TARGET_FIELDS)
            writer.writeheader()
            
            count = 0
            for row in reader:
                clean_row = {k: row.get(k, "") for k in TARGET_FIELDS}
                writer.writerow(clean_row)
                count += 1
                
            print(f"   ✅ Đã tạo: {clean_file.name} ({count} dòng)")
            
    except Exception as e:
        print(f"   ❌ Lỗi khi xử lý file {raw_file.name}: {e}")

def run_batch_all():
    """Chế độ quét và clean tất cả các file chưa được xử lý"""
    raw_dir, clean_dir = setup_paths()
    
    if not raw_dir.exists():
        print(f"❌ Thư mục {raw_dir} không tồn tại.")
        return

    # Lấy danh sách tất cả file csv và sắp xếp theo tên
    all_raw_files = sorted(list(raw_dir.glob("*.csv")))
    
    if not all_raw_files:
        print("❌ Không tìm thấy file CSV nào trong data/raw/")
        return

    print(f"🔍 Tìm thấy {len(all_raw_files)} file raw. Bắt đầu kiểm tra...")
    
    processed_count = 0
    skipped_count = 0

    for raw_path in all_raw_files:
        # Tạo tên file clean tương ứng: thay 'raw' thành 'clean'
        clean_name = raw_path.name.replace("raw", "clean")
        
        # Nếu tên file không có chữ "raw", thêm tiền tố clean_
        if clean_name == raw_path.name:
            clean_name = f"clean_{raw_path.name}"
            
        clean_path = clean_dir / clean_name

        # LOGIC QUAN TRỌNG: Kiểm tra tồn tại
        if clean_path.exists():
            skipped_count += 1
            # print(f"   ⏭️  Bỏ qua (đã có): {clean_name}") # Bỏ comment nếu muốn hiện chi tiết
        else:
            print(f"🚀 Đang xử lý: {raw_path.name} ...")
            clean_file(raw_path, clean_path)
            processed_count += 1

    print("-" * 30)
    print(f"🎉 HOÀN TẤT BATCH JOB!")
    print(f"   - Đã xử lý mới: {processed_count} file")
    print(f"   - Đã bỏ qua (cũ): {skipped_count} file")

# ==========================================
# 3. KHU VỰC CHẠY CHƯƠNG TRÌNH
# ==========================================
if __name__ == "__main__":
    repo_root = get_repo_root()
    raw_dir = repo_root / "data" / "raw"
    clean_dir = repo_root / "data" / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)

    # ƯU TIÊN 1: Chạy file cụ thể (nếu có điền tên)
    if SPECIFIC_FILENAME and SPECIFIC_FILENAME.strip():
        print(f"🎯 CHẾ ĐỘ HARD CODE: Chỉ xử lý {SPECIFIC_FILENAME}")
        input_path = raw_dir / SPECIFIC_FILENAME
        if input_path.exists():
            output_name = input_path.name.replace("raw", "clean")
            clean_file(input_path, clean_dir / output_name)
        else:
            print(f"❌ Không tìm thấy file {SPECIFIC_FILENAME}")

    # ƯU TIÊN 2: Chạy Batch (Mặc định)
    else:
        run_batch_all()