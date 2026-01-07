import sys
import os

# --- HACK ĐƯỜNG DẪN ---
# Giúp Python tìm thấy folder 'analysis' khi chạy file này trực tiếp
# Logic: Lấy thư mục cha của thư mục chứa file này (tức là thư mục gốc dự án)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

# Import module nội bộ
from analysis import utils
# Giả sử bạn đã tạo các file charts/pair_a.py, pair_b.py...
# Nếu chưa có file nào thì comment dòng import đó lại để không bị lỗi
try:
    from analysis.charts import pair_a, pair_b, pair_c
except ImportError as e:
    print(f"⚠️ Cảnh báo Import: {e}")
    print("-> Hãy chắc chắn bạn đã tạo file pair_a.py, pair_b.py trong folder analysis/charts")

def main():
    print("🚀 BẮT ĐẦU QUÁ TRÌNH PHÂN TÍCH DỮ LIỆU")
    print("="*40)
    
    # 1. Thiết lập giao diện
    utils.setup_style()
    
    # 2. Load dữ liệu
    df = utils.load_master_data()
    if df is None: 
        print("❌ Dừng chương trình do không có dữ liệu.")
        return

    print("="*40)

    # 3. Chạy lần lượt từng cặp phân tích
    # Dùng try-except cho từng cặp để lỗi 1 cái không làm chết cả chương trình
    
    # --- CẶP A ---
    try:
        if 'pair_a' in globals():
            print("\n--- [CẶP A] Phân tích Tổng quan ---")
            pair_a.run_analysis(df)
        else:
            print("\n⚠️ [SKIP] Cặp A chưa được import.")
    except Exception as e:
        print(f"❌ Lỗi Cặp A: {e}")

    # --- CẶP B ---
    try:
        if 'pair_b' in globals():
            print("\n--- [CẶP B] Phân tích Chi tiết ---")
            pair_b.run_analysis(df)
        else:
            print("\n⚠️ [SKIP] Cặp B chưa được import.")
    except Exception as e:
        print(f"❌ Lỗi Cặp B: {e}")

    # --- CẶP C ---
    try:
        if 'pair_c' in globals():
            print("\n--- [CẶP C] Phân tích Nâng cao ---")
            pair_c.run_analysis(df)
        else:
            print("\n⚠️ [SKIP] Cặp C chưa được import.")
    except Exception as e:
        print(f"❌ Lỗi Cặp C: {e}")
    
    print("\n" + "="*40)
    print("✅ HOÀN TẤT TOÀN BỘ QUÁ TRÌNH!")

if __name__ == "__main__":
    main()