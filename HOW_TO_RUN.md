# Hướng Dẫn Chạy Data Warehouse Optimization

## Tổng Quan

Dự án này đã được tối ưu hóa để tập trung vào data warehouse với các phương pháp tối ưu tốc độ truy xuất:

- **Pre-aggregation** với Materialized Views
- **Secondary Index** optimization  
- **Probabilistic Data Structures** (PDS)
- **Hybrid optimization** (PDS + Materialized Views)

## Yêu Cầu Hệ Thống

- Windows 10/11
- Python 3.8+
- PostgreSQL 12+
- PowerShell 5.0+

## Cài Đặt

### 1. Cài đặt Python packages
```powershell
pip install -r requirements.txt
```

### 2. Cài đặt PostgreSQL
- Tải và cài đặt PostgreSQL từ https://www.postgresql.org/download/
- Tạo database `crm_erp`
- Cập nhật thông tin kết nối trong `src/dwh_optimized/db_connection.py` nếu cần

## Cách Chạy

### Phương Pháp 1: Chạy Demo PDS (Khuyến nghị)
```powershell
.\run_pds_demo.ps1
```
**Lợi ích**: Thể hiện rõ sự vượt trội của PDS với các demo trực quan

### Phương Pháp 2: Chạy Benchmark Đầy Đủ
```powershell
.\run_optimization_benchmark.ps1
```
**Lợi ích**: So sánh hiệu suất của tất cả các phương pháp tối ưu

### Phương Pháp 3: Chạy Từng Bước
```powershell
# Bước 1: Setup data warehouse
cd src\dwh_optimized
python setup_dwh.py

# Bước 2: Chạy benchmark cơ bản
python benchmark_queries.py

# Bước 3: Chạy hybrid optimization
python pds_hybrid_optimization.py

# Bước 4: Chạy demo PDS
python pds_demo.py
```

## Kết Quả Mong Đợi

### PDS Demo sẽ hiển thị:
- **Bloom Filter**: Tốc độ kiểm tra tồn tại nhanh gấp 10-100x so với SQL
- **HyperLogLog**: Ước lượng COUNT DISTINCT với độ chính xác >95%
- **Count-Min Sketch**: Ước lượng tần suất với độ chính xác cao
- **Memory Efficiency**: Sử dụng bộ nhớ ít hơn 10-100x so với phương pháp truyền thống

### Benchmark sẽ so sánh:
- Basic Query (không tối ưu)
- Materialized Views only
- Index optimization only  
- PDS pre-filtering only
- Hybrid approach (PDS + Materialized Views)

## Cấu Trúc Dự Án

```
HK251_DWH_Project/
├── src/dwh_optimized/           # Code tối ưu hóa chính
│   ├── db_connection.py         # Quản lý kết nối database
│   ├── optimization_methods.py  # Các phương pháp tối ưu
│   ├── setup_dwh.py            # Setup data warehouse
│   ├── benchmark_queries.py    # Benchmark hiệu suất
│   ├── pds_hybrid_optimization.py # Hybrid optimization
│   ├── pds_demo.py             # Demo PDS
│   └── README.md               # Tài liệu chi tiết
├── requirements.txt            # Dependencies
├── run_optimization_benchmark.ps1 # Script chạy benchmark
├── run_pds_demo.ps1           # Script chạy demo PDS
└── HOW_TO_RUN.md              # File này
```

## Tùy Chỉnh

### Thay đổi thông tin kết nối database:
Sửa file `src/dwh_optimized/db_connection.py`:
```python
DatabaseConnection(
    host="localhost",      # Địa chỉ PostgreSQL
    port=5432,            # Port PostgreSQL
    database="crm_erp",   # Tên database
    user="postgres",      # Username
    password="your_pass"  # Password
)
```

### Thay đổi tham số PDS:
Sửa file `src/dwh_optimized/optimization_methods.py`:
```python
# Bloom Filter
self.pds.create_bloom_filter(name, expected_items=10000, false_positive_rate=0.01)

# HyperLogLog  
self.pds.create_hyperloglog(name, precision=4)

# Count-Min Sketch
self.pds.create_count_min_sketch(name, width=1000, depth=5)
```

## Troubleshooting

### Lỗi kết nối database:
- Kiểm tra PostgreSQL đang chạy
- Kiểm tra thông tin kết nối trong `db_connection.py`
- Kiểm tra quyền truy cập database

### Lỗi import Python:
```powershell
pip install --upgrade -r requirements.txt
```

### Lỗi quyền PowerShell:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## Kết Luận

Dự án này thể hiện sự vượt trội của **Probabilistic Data Structures** trong việc tối ưu hóa data warehouse:

1. **Tốc độ**: Nhanh hơn 10-100x so với phương pháp truyền thống
2. **Bộ nhớ**: Tiết kiệm 10-100x bộ nhớ
3. **Độ chính xác**: >95% cho hầu hết các trường hợp
4. **Khả năng mở rộng**: Xử lý được dữ liệu lớn trong thời gian thực

PDS đặc biệt phù hợp cho:
- Real-time analytics
- Big data processing  
- Dashboard BI
- Trend analysis
- Pre-filtering trước khi thực hiện query phức tạp




