

# Car Price Analysis Pipeline

## 1. Giới thiệu
Project này xây dựng pipeline thu thập và phân tích dữ liệu giá xe cũ từ nhiều website khác nhau, nhằm hỗ trợ người dùng tra cứu, so sánh và lựa chọn mức giá xe phù hợp.

Pipeline gồm các giai đoạn:
1. Thu thập dữ liệu (Crawler)
2. Tiền xử lý & làm sạch dữ liệu
3. Phân tích dữ liệu
4. Trực quan hóa & báo cáo

Mỗi website được thu thập bởi một crawler riêng, do một cặp phụ trách.

---

## 2. Chuẩn bị môi trường

### 2.1 Cài đặt Python
Yêu cầu:
- Python >= 3.9

Kiểm tra phiên bản:
```bash
python --version
```

### 2.2 Tạo môi trường ảo

#### Bước 1: Tạo môi trường ảo
```bash
python -m venv venv
```
#### Bước 2: Kích hoạt môi trường ảo
- Trên Windows:
```bash
venv\Scripts\activate
```
Sau khi kích hoạt, tên môi trường venv sẽ xuất hiện ở đầu dòng lệnh.

### 2.3 Cài đặt thư viện
```bash
pip install -r requirements.txt
```

## 3. Cấu trúc thư mục chính

car_price_pipeline/
├── configs/            # File cấu hình (URL, DB)
├── crawler/            # Code thu thập dữ liệu
├── preprocessing/      # Code làm sạch & chuẩn hóa
├── analysis/           # Notebook phân tích dữ liệu
├── dashboard/          # Web app (Streamlit)
├── report/             # Báo cáo & slide
├── logs/               # Log chương trình
├── data/
│   ├── raw/            # Dữ liệu thô (Bronze)
│   ├── cleaned/        # Dữ liệu đã làm sạch (Silver)
│   └── master/         # Dữ liệu tổng hợp cuối (Gold)
└── README.md


### Chi tiết thư mục

car_price_pipeline/
│
├── configs/                # Cấu hình 
│   ├── urls.yaml           # Link các trang web cần cào
│   └── settings.json       # Cấu hình khác
│
├── data/                   # KHO DỮ LIỆU 
│   ├── raw/                # Dữ liệu thô (Kết quả của 3 cặp Crawler)
│   │   ├── bonbanh_raw.csv
│   │   ├── chotot_raw.csv
│   │   └── carmudi_raw.csv
│   ├── cleaned/            # Dữ liệu sạch từng phần (Sau khi xử lý)
│   └── master/             # Dữ liệu tổng hợp cuối cùng (Dùng cho Web/Báo cáo)
│       └── final_dataset.csv
│
├── crawler/                # CODE THU THẬP (Chia theo 3 cặp)
│   ├── __init__.py
│   ├── utils.py            # Hàm chung (fake user-agent, save csv...)
│   ├── bonbanh_spider.py   # Code của Cặp A
│   ├── chotot_spider.py    # Code của Cặp C
│   └── otocomvn_spider.py   # Code của Cặp B
│
├── preprocessing/          # CODE XỬ LÝ DỮ LIỆU
│   ├── cleaning.py         # Làm sạch (xử lý null, duplicate)
│   └── integration.py      # Gộp 3 file csv thành 1 file master
│
├── analysis/               # PHÂN TÍCH & VISUALIZATION
│   ├── eda_notebook.ipynb  # File Jupyter phân tích khám phá
│   └── charts/             # Lưu ảnh biểu đồ xuất ra
│
├── dashboard/              # ỨNG DỤNG WEB
│   ├── app.py              # File chạy Streamlit
│   └── components.py       # Các thành phần giao diện
│
├── report/                 # TÀI LIỆU BÁO CÁO
│   ├── slides/
│   └── final_report.docx
│
├── logs/                   # LOG HOẠT ĐỘNG
│   └── pipeline.log
│
├── .gitignore              # Loại bỏ file rác khi up lên Git
├── requirements.txt        # Danh sách thư viện cần cài
└── README.md               # Hướng dẫn chạy dự án



## 4. Quy ước chung

- Không chỉnh sửa trực tiếp dữ liệu trong data/raw/

- Tất cả crawler phải xuất dữ liệu theo cùng schema

- Mỗi cặp chỉ chạy crawler của website mình phụ trách

- Không hardcode URL trong code, URL phải nằm trong configs/urls.yaml

Tất cả các crawler trong project phải xuất dữ liệu theo cùng một schema
để đảm bảo dữ liệu từ nhiều website có thể được gộp và xử lý thống nhất.

### Danh sách các trường dữ liệu chính:

| Tên cột | Mô tả |
|------|------|
| brand | Hãng xe |
| model | Dòng xe |
| year | Năm sản xuất |
| price | Giá xe (VNĐ, số nguyên) |
| mileage | Số km đã đi |
| fuel | Loại nhiên liệu |
| location | Tỉnh/Thành phố |
| color | Màu xe |
| source | Website crawl dữ liệu |
| crawl_date | Ngày crawl dữ liệu |

### Quy ước bắt buộc
- `price`: đơn vị VNĐ, kiểu số (int)
- `year`: định dạng YYYY
- `mileage`: đơn vị km, kiểu số
- Tên cột dùng tiếng Anh, chữ thường, snake_case
- Không thêm hoặc đổi tên cột nếu chưa thống nhất toàn nhóm

Schema này được áp dụng cho:
- Dữ liệu thô (`data/raw/`)
- Dữ liệu đã làm sạch (`data/cleaned/`)
- Dữ liệu tổng hợp (`data/master/`)


## 5. Hướng dẫn chạy chương trình theo từng giai đoạn

### 5.1 Giai đoạn 1 – Thu thập dữ liệu (Crawler)

Mỗi cặp chỉ chạy crawler của website được phân công.
#### Cặp A – Bonbanh
```bash
python crawler/bonbanh_spider.py
```

#### Cặp B – Oto.com.vn
```bash
python crawler/otocomvn_spider.py
```
#### Cặp C – Chotot
```bash
python crawler/chotot_spider.py
```
Kết quả:

- Dữ liệu thô được lưu trong thư mục:
```bash
data/raw/
```

- Tên file có dạng:
```bash
{website}_YYYYMMDD.csv
```

### 5.2 Giai đoạn 2 – Tiền xử lý & làm sạch dữ liệu
#### Bước 1: Làm sạch dữ liệu
```bash
python preprocessing/cleaning.py
```
Công việc chính:

- Xử lý dữ liệu null

- Xóa dữ liệu trùng lặp

- Chuẩn hóa kiểu dữ liệu

Kết quả:
```bash
data/cleaned/
```

#### Bước 2: Biến đổi & chuẩn hóa dữ liệu

```bash
python preprocessing/transformation.py
```

Công việc chính:

- Chuẩn hóa giá 

- Chuẩn hóa số km

- Tạo các thuộc tính mới (tuổi xe, giá/năm sử dụng)

Kết quả:
```bash
data/master/car_price_master.csv
```

### 5.3 Giai đoạn 3 – Phân tích dữ liệu
Mở Jupyter Notebook:
```bash
jupyter notebook
```
Chạy các notebook trong thư mục analysis/ theo thứ tự:

1. 1_eda_distribution.ipynb – Phân tích phân bố dữ liệu

2. 2_price_depreciation.ipynb – Phân tích khấu hao giá

3. 3_correlation_analysis.ipynb – Phân tích tương quan

### 5.4 Giai đoạn 4 – Trực quan hóa & Demo Web App
Chạy ứng dụng Streamlit:
```bash
streamlit run dashboard/app.py
```

Ứng dụng cho phép:

- Tra cứu giá xe

- So sánh giá giữa các xe

- Quan sát xu hướng giá thị trường

## 6. Log & kiểm tra lỗi

- Log chương trình được lưu tại:


```bash
logs/pipeline.log
```

- Kiểm tra log nếu crawler hoặc preprocessing gặp lỗi.

## 7. Báo cáo & thuyết trình

- Báo cáo Word: report/Final_Report.docx

- Slide thuyết trình: report/slides/
## 8. Ghi chú cuối

- Mỗi giai đoạn đều có output riêng để dễ kiểm tra

- Dữ liệu được xử lý theo pipeline Bronze → Silver → Gold

- Project được thiết kế để dễ mở rộng thêm website mới