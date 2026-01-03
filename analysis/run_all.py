# analysis/run_all.py
import sys
import os

# Hack để python tìm thấy module analysis
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis import utils
from analysis.charts import pair_a, pair_b, pair_c

def main():
    print("🚀 BẮT ĐẦU QUÁ TRÌNH PHÂN TÍCH & TRỰC QUAN HÓA")
    
    # 1. Thiết lập giao diện
    utils.setup_style()
    
    # 2. Load dữ liệu một lần duy nhất
    df = utils.load_master_data()
    if df is None: return

    # 3. Chạy lần lượt từng cặp
    try:
        # Cặp A
        pair_a.run_analysis(df)
        
        # Cặp B
        pair_b.run_analysis(df)
        
        # Cặp C
        pair_c.run_analysis(df)
        
    except Exception as e:
        print(f"\n❌ CÓ LỖI XẢY RA TRONG QUÁ TRÌNH CHẠY: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n✅ HOÀN TẤT TOÀN BỘ!")

if __name__ == "__main__":
    main()