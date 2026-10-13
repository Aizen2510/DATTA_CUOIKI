# HỆ THỐNG XỬ LÝ DỮ LIỆU & DATA WAREHOUSE (MOVIELENS + TMDB)

Dự án xây dựng pipeline kỹ thuật dữ liệu hoàn chỉnh từ thu thập, lưu trữ dữ liệu thô (Data Lake), chuyển đổi chuẩn hóa (ETL) theo mô hình hình sao (Star Schema), lưu trữ vào kho dữ liệu (Data Warehouse) và điều phối tự động với Apache Airflow.

---

## 1. Cấu trúc thư mục & Giải thích tác dụng các file

```text
Data_cuoi_ky/
├── docker-compose.yml          # Quản lý container MinIO (S3 Object Storage) & PostgreSQL (Data Warehouse)
├── fetch_raw_data.py           # Thu thập (Crawl) dữ liệu bổ sung từ TMDB API
├── ingest_to_minio.py          # Nạp dữ liệu thô từ máy local lên MinIO Data Lake
├── etl_process.py              # Script ETL độc lập: Trích xuất, làm sạch, biến đổi và nạp vào PostgreSQL
├── start_airflow.sh            # Script tiện ích 1 chạm để khởi động Airflow Web UI & Scheduler
├── dags/
│   └── movielens_etl_dag.py    # Định nghĩa DAG Airflow điều phối tự động toàn bộ luồng ETL
├── raw_data/                   # Chứa dữ liệu gốc (movies.csv, ratings.csv, links.csv, tmdb_movies_raw.json)
├── minio_storage/              # Dữ liệu volume ánh xạ từ container MinIO
├── dw_data/                    # Dữ liệu volume ánh xạ từ container PostgreSQL Data Warehouse
├── airflow_home/               # Metadata DB và cấu hình chạy Airflow
└── venv/                       # Môi trường ảo Python (đã cài đủ airflow, pandas, sqlalchemy, minio,...)
```

### Chi tiết vai trò từng file:

| Tên File | Vai trò & Chức năng chi tiết |
|---|---|
| **[docker-compose.yml](docker-compose.yml)** | Khởi tạo hạ tầng:<br>• **MinIO** (cổng `9000` cho S3 API, `9001` cho Web Console).<br>• **PostgreSQL** (cổng `5434:5432`, DB `dw_movies`) làm Data Warehouse lưu trữ bảng Fact và Dimension. |
| **[fetch_raw_data.py](fetch_raw_data.py)** | Đọc danh sách `tmdbId` từ `links.csv`, gọi REST API của TMDB để lấy metadata phim (popularity, vote_average, overview, genres...) và lưu thành file bán cấu trúc `raw_data/tmdb_movies_raw.json`. |
| **[ingest_to_minio.py](ingest_to_minio.py)** | Đóng vai trò **Raw Ingestion Layer**: Quét các file trong `raw_data/`, tự động tạo bucket `raw-datalake` trên MinIO nếu chưa có và upload phân loại vào 2 thư mục prefix:<br>• `/movielens`: các file structured `.csv`<br>• `/tmdb`: file semi-structured `.json` |
| **[etl_process.py](etl_process.py)** | Thực hiện quy trình ETL chuẩn:<br>• **Extract**: Đọc trực tiếp từ MinIO Data Lake.<br>• **Transform**: Khử trùng lặp, tách năm/thập niên phát hành, ghép nối thông tin TMDB, chuẩn hóa theo mô hình Star Schema (`dim_user`, `dim_time`, `dim_genre`, `dim_movie`, `fact_rating`).<br>• **Load**: Nạp toàn bộ vào PostgreSQL Data Warehouse. |
| **[dags/movielens_etl_dag.py](dags/movielens_etl_dag.py)** | Pipeline điều phối (Orchestration) của **Apache Airflow**: Định nghĩa DAG `movielens_data_warehouse_etl` để tự động hóa hoặc lập lịch chạy quy trình ETL. |
| **[start_airflow.sh](start_airflow.sh)** | Script Shell hỗ trợ khởi chạy nhanh Apache Airflow Standalone chỉ bằng một câu lệnh, tự động in thông tin tài khoản đăng nhập `admin/admin`. |

---

## 2. Thông tin tài khoản & Cổng dịch vụ

| Dịch vụ | Địa chỉ truy cập | Username | Password | Database |
|---|---|---|---|---|
| **Airflow Web UI** | `http://localhost:8080` | `admin` | `admin` | - |
| **MinIO Web Console** | `http://localhost:9001` | `minioadmin` | `minioadmin` | - |
| **MinIO S3 API** | `http://localhost:9000` | `minioadmin` | `minioadmin` | Bucket: `raw-datalake` |
| **PostgreSQL DW** | `localhost:5434` | `dw_user` | `dw_password` | `dw_movies` |

---

## 3. Hướng dẫn chạy dự án từ A - Z

Mở Terminal tại thư mục gốc của dự án (`/home/aizen/Data_cuoi_ky`) và thực hiện theo thứ tự các bước sau:

### Bước 1: Khởi động Hạ tầng Docker (MinIO & PostgreSQL)
```bash
docker compose up -d
```
> Kiểm tra cả 2 container đang chạy bằng: `docker ps`

---

### Bước 2: Kích hoạt môi trường ảo Python
Dự án dùng chung 1 thư mục `venv` duy nhất đã cài đặt đầy đủ dependencies:
```bash
source venv/bin/activate
```

---

### Bước 3 (Tùy chọn): Thu thập dữ liệu mới từ TMDB API
*(Chỉ chạy khi bạn muốn cào lại hoặc cập nhật dữ liệu JSON mới từ TMDB)*
```bash
python fetch_raw_data.py
```
> Dữ liệu kết quả sẽ được ghi vào file `raw_data/tmdb_movies_raw.json`.

---

### Bước 4: Đẩy dữ liệu thô vào MinIO Data Lake (Ingestion)
```bash
python ingest_to_minio.py
```
> Kết quả: Tải 5 file (`movies.csv`, `ratings.csv`, `links.csv`, `tags.csv`, `tmdb_movies_raw.json`) lên bucket `raw-datalake` trên MinIO. Bạn có thể mở `http://localhost:9001` để kiểm tra.

---

### Bước 5: Thực hiện ETL nạp vào Data Warehouse

Bạn có thể chọn **1 trong 2 cách** sau:

#### Cách 5.1: Chạy trực tiếp qua Python script (Nhanh nhất)
```bash
python etl_process.py
```

#### Cách 5.2: Chạy điều phối qua Apache Airflow (Theo yêu cầu đề bài)

**Cách A - Chạy test nhanh DAG qua Terminal:**
```bash
export AIRFLOW_HOME="$(pwd)/airflow_home"
airflow dags test movielens_data_warehouse_etl 2026-01-01 --subdir dags
```

**Cách B - Bật giao diện Airflow Web UI:**
```bash
./start_airflow.sh
```
1. Truy cập: `http://localhost:8080`
2. Đăng nhập với: `admin` / `admin`
3. Tìm DAG **`movielens_data_warehouse_etl`**, gạt nút sang **ON** (Unpause) và ấn nút **Trigger DAG** (biểu tượng Play) ở góc phải để thực thi.

---

### Bước 6: Kiểm tra kết quả trong Data Warehouse (PostgreSQL)

Chạy lệnh sau để kiểm đếm số bản ghi trong từng bảng:
```bash
docker exec postgres_dw psql -U dw_user -d dw_movies -c "
SELECT 'dim_user' as tbl, count(*) FROM dim_user
UNION ALL SELECT 'dim_movie', count(*) FROM dim_movie
UNION ALL SELECT 'dim_time', count(*) FROM dim_time
UNION ALL SELECT 'dim_genre', count(*) FROM dim_genre
UNION ALL SELECT 'fact_rating', count(*) FROM fact_rating;
"
```

**Kết quả chuẩn:**
- `dim_user`: **610** người dùng
- `dim_movie`: **9,742** bộ phim
- `dim_time`: **7,761** mốc thời gian
- `dim_genre`: **19** thể loại phim
- `fact_rating`: **100,836** bản ghi đánh giá
