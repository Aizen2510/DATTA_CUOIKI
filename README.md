# HỆ THỐNG XỬ LÝ DỮ LIỆU & DATA WAREHOUSE (MOVIELENS + TMDB)

Phần Recommendation đã có CLI LightFM + WARP và baseline Most Popular.
Xem [báo cáo, kết quả thực tế và hướng dẫn chạy](RECOMMENDATION_AUDIT.md).
Để học từng bước, xem [luồng chi tiết, các trường và thuật toán Recommendation](RECOMMENDATION_FLOW.md).
Trên môi trường đã chuẩn bị: `.venv/bin/python recommend.py --user-id 15 --top-k 10`.
Lần chạy ML đã kiểm chứng dùng Raw Storage local → ETL → PostgreSQL; MinIO chưa khả dụng.

Dự án xây dựng quy trình Kỹ thuật Dữ liệu (Data Engineering) hoàn chỉnh: từ thu thập đa nguồn, lưu trữ hồ dữ liệu thô (Data Lake), xử lý chuẩn hóa ETL theo mô hình hình sao (Star Schema có Bridge Table), lưu trữ vào Kho dữ liệu (Data Warehouse) trên PostgreSQL, đến điều phối và kiểm định chất lượng tự động bằng Apache Airflow.

---

## 1. Tác dụng của Kho Dữ liệu (Data Warehouse) này

Kho dữ liệu này được thiết kế theo chuẩn OLAP nhằm phục vụ 2 mục đích cốt lõi:

### 1.1. Phục vụ Phân tích Kinh doanh & Trực quan hóa (BI / Analytics)
- **Phân tích hành vi người dùng:** Tần suất đánh giá, thói quen cho điểm (khắt khe hay dễ dãi), mức độ gắn bó theo thời gian.
- **Phân tích xu hướng phim & thị hiếu:** Thống kê top phim, điểm đánh giá trung bình theo từng thập niên/năm phát hành.
- **Phân tích đa chiều theo thể loại:** Nhờ bảng cầu nối `bridge_movie_genre`, người phân tích có thể lọc và mổ xẻ chính xác doanh số/lượt xem theo từng thể loại mà không làm sai lệch số liệu của các phim đa thể loại.
- **Phân tích xu hướng theo chu kỳ thời gian:** Đánh giá khối lượng tương tác theo khung giờ trong ngày, ngày trong tuần, quý và năm.

### 1.2. Phục vụ Khai phá Dữ liệu & Học máy (Machine Learning / Recommendation System)
- **Lọc cộng tác (Collaborative Filtering):** Bảng `fact_rating` cung cấp sẵn ma trận tương tác User – Item chuẩn hóa (610 người dùng $\times$ 9.724 bộ phim với 100.836 tương tác) làm đầu vào hoàn hảo cho các mô hình Matrix Factorization (SVD, NMF), ALS, hoặc Neural Collaborative Filtering.
- **Gợi ý dựa trên nội dung (Content-based Filtering):** Bảng `dim_movie` và `bridge_movie_genre` kết hợp thể loại, năm phát hành cùng trường mô tả nội dung `overview` từ TMDB, sẵn sàng cho các kỹ thuật TF-IDF, CountVectorizer hoặc sinh Movie Embeddings.

---

## 2. Mô hình Kho Dữ liệu (Star Schema + Bridge Table)

Kho dữ liệu được tổ chức theo kiến trúc **Mô hình hình sao mở rộng (Star Schema with Bridge Table)** với đầy đủ ràng buộc khóa chính (PK), khóa ngoại (FK) và chỉ mục B-tree (Index):

```mermaid
erDiagram
    dim_user ||--o{ fact_rating : "user_id"
    dim_movie ||--o{ fact_rating : "movie_id"
    dim_time ||--o{ fact_rating : "time_id"
    dim_movie ||--o{ bridge_movie_genre : "movie_id"
    dim_genre ||--o{ bridge_movie_genre : "genre_id"

    dim_user {
        int user_id PK
    }
    dim_movie {
        int movie_id PK
        text title
        text genres
        int release_year
        int release_decade
        int tmdb_id
        float popularity
        float vote_average
        int vote_count
        text overview
        boolean has_tmdb_metadata
    }
    dim_genre {
        int genre_id PK
        varchar genre_name UK
    }
    bridge_movie_genre {
        int movie_id PK, FK
        int genre_id PK, FK
    }
    dim_time {
        bigint time_id PK
        timestamp hour_timestamp
        int hour
        int day
        int month
        int quarter
        int year
        int day_of_week
        varchar day_name
    }
    fact_rating {
        int user_id PK, FK
        int movie_id PK, FK
        bigint time_id FK
        float rating
        timestamp rating_timestamp
    }
```

### Chi tiết các bảng trong Data Warehouse:

| Tên bảng | Loại bảng | Số bản ghi | Khóa chính (PK) | Mô tả chi tiết |
|---|---|---:|---|---|
| **`dim_user`** | Dimension | 610 | `user_id` | Đại diện cho các tài khoản người dùng thực hiện đánh giá. |
| **`dim_movie`** | Dimension | 9.742 | `movie_id` | Chứa thông tin phim chuẩn hóa (tiêu đề sạch, năm, thập niên) ghép với metadata TMDB (`popularity`, `vote_average`, `overview`, `has_tmdb_metadata`). |
| **`dim_genre`** | Dimension | 19 | `genre_id` | Danh mục 19 thể loại phim chuẩn hóa (Action, Comedy, Drama,...). |
| **`bridge_movie_genre`** | Bridge (Cầu nối) | 22.050 | `(movie_id, genre_id)` | Giải quyết quan hệ Nhiều - Nhiều (N-N) giữa Phim và Thể loại, hỗ trợ Power BI phân tích thể loại chính xác. |
| **`dim_time`** | Dimension | 7.761 | `time_id` | Grain chuẩn hóa theo giờ (`%Y%m%d%H`), cung cấp các cấp độ ngày, tháng, quý, năm, thứ trong tuần. |
| **`fact_rating`** | Fact (Sự kiện) | 100.836 | `(user_id, movie_id)` | Bảng sự kiện chứa điểm số (`rating`), mốc thời gian chi tiết (`rating_timestamp`), nối tới 3 chiều: user, movie, time. |

---

## 3. Cấu hình & Thông tin kết nối Database

| Dịch vụ | Địa chỉ | Port | Tài khoản và mật khẩu | Database / Bucket |
|---|---|---:|---|---|---|
| **PostgreSQL DW** | `localhost` | **`5434`** | Set in local `.env` | `dw_movies` |
| **MinIO Data Lake (Web)** | `http://localhost:9001` | `9001` | Set in local `.env` | Bucket: `raw-datalake` |
| **MinIO S3 API** | `localhost:9000` | `9000` | Set in local `.env` | `raw-datalake` |
| **Airflow Web UI** | `http://localhost:8080` | `8080` | Set in local `.env` | DAG: `movielens_data_warehouse_etl` |

*Chuỗi kết nối chuẩn (Connection String):*
```text
postgresql://<DW_USER>:<DW_PASSWORD>@localhost:5434/<DW_DATABASE>
```

---

## 4. Cách sử dụng Kho Dữ liệu (Data Warehouse)

### 4.1. Kết nối với Power BI để làm Dashboard
1. Mở **Power BI Desktop** $\rightarrow$ chọn **Get Data** $\rightarrow$ **PostgreSQL database**.
2. **Server:** `localhost:5434` | **Database:** `dw_movies` | Chế độ: **Import**.
3. Nhập user/password đã cấu hình trong `.env` (với volume PostgreSQL cũ, phải dùng đúng tài khoản đã khởi tạo volume đó).
4. Tích chọn toàn bộ 6 bảng (`dim_user`, `dim_movie`, `dim_genre`, `bridge_movie_genre`, `dim_time`, `fact_rating`).
5. **Cấu hình Model View:**
   - Kéo các đường nối khóa ngoại từ các bảng Dim tới Fact theo diagram ở Mục 2.
   - Để lọc Fact theo Thể loại: Click đúp vào đường nối giữa `dim_movie` và `bridge_movie_genre` $\rightarrow$ đổi **Cross filter direction** thành **Both**.
6. **Bộ DAX mẫu khuyến nghị:**
   ```dax
   Total Ratings = COUNTROWS(fact_rating)
   Average Rating = ROUND(AVERAGE(fact_rating[rating]), 2)
   Total Movies = DISTINCTCOUNT(dim_movie[movie_id])
   Total Active Users = DISTINCTCOUNT(fact_rating[user_id])
   ```

### 4.2. Truy vấn trực tiếp bằng SQL trong VS Code / DBeaver
Dự án đã chuẩn bị sẵn file [query_dw.sql](query_dw.sql) chứa các câu truy vấn mẫu:
- **Top 10 phim được đánh giá cao nhất:**
  ```sql
  SELECT m.title, m.release_year, ROUND(AVG(f.rating)::numeric, 2) AS avg_rating, COUNT(f.rating) AS total_ratings
  FROM fact_rating f
  JOIN dim_movie m ON f.movie_id = m.movie_id
  GROUP BY m.movie_id, m.title, m.release_year
  HAVING COUNT(f.rating) >= 50
  ORDER BY avg_rating DESC, total_ratings DESC LIMIT 10;
  ```
- **Thống kê phân phối phim theo thể loại qua bảng Bridge:**
  ```sql
  SELECT g.genre_name, COUNT(b.movie_id) AS total_movies
  FROM dim_genre g
  JOIN bridge_movie_genre b ON g.genre_id = b.genre_id
  GROUP BY g.genre_id, g.genre_name ORDER BY total_movies DESC;
  ```

### 4.3. Đọc dữ liệu vào Python phục vụ Machine Learning
```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://dw_user:dw_password@localhost:5434/dw_movies")

# Lấy ma trận đánh giá cho Collaborative Filtering
df_ratings = pd.read_sql("SELECT user_id, movie_id, rating FROM fact_rating;", engine)

# Lấy thông tin phim và thể loại cho Content-based Filtering
df_movies = pd.read_sql("SELECT movie_id, title, genres, release_year, overview FROM dim_movie;", engine)
```

---

## 5. Quy trình vận hành Pipeline ETL tự động

Quy trình xử lý snapshot dữ liệu hiện có được điều phối qua 6 task Apache Airflow:

```mermaid
graph LR
    A[1. ingest_raw_to_minio] --> B[2. extract_from_minio]
    B --> C[3. clean_and_transform]
    C --> D[4. load_dimension_tables]
    D --> E[5. load_fact_rating]
    E --> F[6. validate_data_warehouse]
```

1. **`ingest_raw_to_minio`:** Upload snapshot trong `raw_data/` lên MinIO bucket `raw-datalake`.
2. **`extract_from_minio`:** Đọc các object MovieLens/TMDB cần ETL từ MinIO vào `staging/`.
3. **`clean_and_transform`:**
   - Làm sạch Ratings, khử trùng lặp cặp `(user_id, movie_id)`.
   - Chuẩn hóa `dim_time` theo grain giờ nhất quán (`hour_timestamp`).
   - Tách và chuẩn hóa danh mục thể loại `dim_genre` và sinh bảng cầu nối `bridge_movie_genre`.
   - Ghép metadata TMDB, gán cờ `has_tmdb_metadata` và giữ `NULL` điểm vote cho phim chưa có dữ liệu.
4. **`load_dimension_tables`:** Tạo DDL nếu chưa có, sau đó upsert dimensions/bridge theo khóa; không xóa các dòng cũ.
5. **`load_fact_rating`:** Upsert `fact_rating` theo khóa `(user_id, movie_id)` để chạy lại không tạo bản ghi trùng.
6. **`validate_data_warehouse`:** Khâu kiểm định chất lượng tự động (Data Quality Gate):
   - Kiểm tra số lượng bản ghi (> 0).
   - Kiểm tra tính duy nhất và không Null trên tất cả Primary Key.
   - Kiểm tra tính toàn vẹn tham chiếu khóa ngoại (0 orphan records).
   - Kiểm tra miền giá trị hợp lệ của Rating [0.5, 5.0].

---

## 6. Hướng dẫn khởi chạy toàn bộ hệ thống từ đầu

### Bước 1: Tạo file `.env` và khởi động toàn bộ hạ tầng Docker
Trong PowerShell:
```powershell
Copy-Item .env.example .env
notepad .env
.\start_airflow.ps1
```
Thay giá trị mẫu trong `.env` bằng thông tin riêng trước khi chạy script; dùng mật khẩu dạng URL-safe (chữ và số). Không đưa `.env` lên Git. Nếu đã có `dw_data` hoặc `minio_storage`, giữ đúng thông tin đăng nhập đang dùng với volume hiện có; Compose không tự đổi thông tin đăng nhập được lưu trong volume PostgreSQL.

### Bước 2: Kiểm tra và kích hoạt DAG
```powershell
docker compose ps
docker compose logs --tail 100 airflow-scheduler
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```
Truy cập [http://localhost:8080](http://localhost:8080), đăng nhập bằng tài khoản trong `.env`, bật DAG `movielens_data_warehouse_etl` nếu đang paused rồi bấm **Trigger DAG**.

Lịch `@daily` điều phối ETL hằng ngày. Mỗi lần chạy DAG upload snapshot hiện có trong `raw_data/` lên MinIO; việc đó không tải MovieLens mới. `fetch_raw_data.py` chỉ lấy metadata TMDB khi được gọi riêng với `TMDB_API_KEY`.
