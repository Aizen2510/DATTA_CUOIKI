# HỆ THỐNG XỬ LÝ DỮ LIỆU, DATA WAREHOUSE & GỢI Ý PHIM CÁ NHÂN HÓA
### Đề tài: Xây dựng kho dữ liệu đa nguồn phục vụ phân tích hành vi người dùng và gợi ý phim cá nhân hóa (MovieLens + TMDB)
**Repository:** https://github.com/Aizen2510/DATTA_CUOIKI

---

## 1. Giới thiệu Tổng quan Dự án

Dự án xây dựng một quy trình Kỹ thuật Dữ liệu (Data Engineering Pipeline) hoàn chỉnh chuẩn công nghiệp từ đầu đến cuối (End-to-End), bao gồm:
1. **Thu thập đa nguồn (Multi-source Ingestion):** Tích hợp dữ liệu đánh giá phim từ MovieLens 100k và dữ liệu làm giàu siêu dữ liệu (metadata, poster, overview, vote, popularity) từ The Movie Database (TMDB) API.
2. **Lưu trữ hồ dữ liệu thô (Data Lake):** Lưu trữ đối tượng tập trung trên **MinIO Object Storage (S3 API)** hoặc Local Raw Storage theo nguyên tắc giữ nguyên dữ liệu gốc bất biến (immutable raw data).
3. **Xử lý, làm sạch & chuẩn hóa (ETL Pipeline):** Pipeline Python (Pandas/SQLAlchemy) chuyển đổi dữ liệu theo chuẩn mô hình hình sao mở rộng (**Star Schema with Bridge Table**), khử trùng lặp, chuẩn hóa grain thời gian và thể loại.
4. **Lưu trữ Kho dữ liệu (Data Warehouse):** Triển khai trên **PostgreSQL (OLAP schema)** với toàn vẹn tham chiếu nghiêm ngặt (PK, FK, Indexes, Upsert Idempotent).
5. **Điều phối & Kiểm định tự động (Orchestration & Data Quality Gate):** Điều phối tự động bằng **Apache Airflow DAG**, tích hợp chốt chặn chất lượng dữ liệu (Data Quality Gate) trước khi cung cấp cho các ứng dụng hạ nguồn.
6. **Ứng dụng hạ nguồn (Serving Layer):**
   - **Business Intelligence & Trực quan hóa:** Dashboard phân tích kinh doanh trên **Power BI** ([MovieLens_TMDB_BI.pbix](MovieLens_TMDB_BI.pbix)).
   - **Machine Learning & Khai phá dữ liệu:** Hệ thống gợi ý phim cá nhân hóa kết hợp (Hybrid Recommender System) sử dụng **LightFM với WARP Loss** ([recommend.py](recommend.py)) và baseline Most Popular.

---

## 2. Sơ đồ Kiến trúc Tổng quan Hệ thống (System Architecture)

Hệ thống được thiết kế theo kiến trúc chuẩn công nghiệp nhiều tầng (Multi-tier Enterprise Data Architecture), kết hợp chặt chẽ từ thu thập, lưu trữ, xử lý, điều phối tự động cho đến tầng ứng dụng phân tích kinh doanh và gợi ý thông minh:

```mermaid
flowchart LR
    %% ========================================================
    %% 1. NGUỒN DỮ LIỆU
    %% ========================================================
    subgraph SOURCELAYER ["1. Data Source (Nguồn Dữ Liệu)"]
        direction TB
        TMDB["The Movie Database (TMDB)<br/>(REST API / JSON Metadata)"]
    end

    %% ========================================================
    %% 2. INGESTION & DATA LAKE
    %% ========================================================
    subgraph INGESTLAYER ["2. Ingestion & Data Lake (Hồ Dữ Liệu Thô)"]
        direction TB
        INGEST["ingest_to_minio.py<br/>fetch_raw_data.py"]
        MINIO[("MinIO Data Lake (S3 API)<br/>bucket: raw-datalake<br/>Port 9000 / 9001")]
        RAW_LOCAL[("Local Raw Storage<br/>raw_data/")]
    end

    %% ========================================================
    %% 3. ETL ENGINE & STAGING
    %% ========================================================
    subgraph ETLLAYER ["3. ETL Engine & Staging (Xử Lý & Chuẩn Hóa)"]
        direction TB
        STAGING["Staging Area (staging/)<br/>• Trích xuất năm/thập niên từ title<br/>• Khử trùng lặp cặp (user_id, movie_id)<br/>• Chuẩn hóa grain giờ (%Y%m%d%H)<br/>• Tách thể loại & sinh bảng cầu nối N-N<br/>• Làm giàu thông tin & cờ has_tmdb_metadata"]
    end

    %% ========================================================
    %% 4. KHO DỮ LIỆU DATA WAREHOUSE (OLAP)
    %% ========================================================
    subgraph DWLAYER ["4. PostgreSQL Data Warehouse (OLAP - Port 5434)"]
        direction TB
        subgraph STAR_SCHEMA ["Mô Hình Hình Sao Mở Rộng (Star Schema + Bridge Table)"]
            direction TB
            DIMS["Các Bảng Chiều (Dimensions)<br/>• dim_user (610 users)<br/>• dim_movie (9.742 movies)<br/>• dim_genre (19 genres)<br/>• dim_time (7.761 hours)"]
            BMG["Bảng Cầu Nối (Bridge Table)<br/>• bridge_movie_genre (22.050 records)<br/>PK/FK: (movie_id, genre_id)"]
            FR["Bảng Sự Kiện (Fact Table)<br/>• fact_rating (100.836 ratings)<br/>PK: (user_id, movie_id) • FK: time_id"]
        end
    end

    %% ========================================================
    %% 5. DATA QUALITY GATE
    %% ========================================================
    subgraph DQLAYER ["5. Data Quality Gate (Kiểm Định Chất Lượng)"]
        direction TB
        DQ["Kiểm Định Tự Động<br/>• 0 bảng rỗng (> 0 records)<br/>• 0 Orphan Records (Toàn vẹn FK)<br/>• Uniqueness PK & NOT NULL<br/>• Rating hợp lệ: 0.5 <= rating <= 5.0<br/>• Upsert Idempotent an toàn"]
    end

    %% ========================================================
    %% 6. SERVICE LAYER (TẦNG DỊCH VỤ & ỨNG DỤNG)
    %% ========================================================
    subgraph SERVINGLAYER ["6. Service Layer (Tầng Dịch Vụ & Ứng Dụng)"]
        direction TB
        subgraph BI ["Business Intelligence"]
            PBI["Power BI Dashboard<br/>(MovieLens_TMDB_BI.pbix)"]
        end
        subgraph ML ["Machine Learning & RecSys"]
            ML_TEST["File Test Truy Vấn & Đánh Giá<br/>(run_ml.bat / recommend.py)"]
            LFM["Hybrid LightFM (WARP Loss)"]
            BASELINE["Most Popular Fallback Engine"]
        end
    end

    %% ========================================================
    %% 7. HỆ THỐNG ĐIỀU PHỐI (APACHE AIRFLOW)
    %% ========================================================
    subgraph ORCHESTRATION ["Điều Phối Tự Động (Apache Airflow • Port 8080)"]
        AF_DAG["DAG: movielens_data_warehouse_etl (@daily)<br/>Task 1: Ingest ➔ Task 2: Extract ➔ Task 3: Transform ➔ Task 4: Load Dims ➔ Task 5: Load Fact ➔ Task 6: Validate"]
    end

    %% LUỒNG DỮ LIỆU TUẦN TỰ TỪ TRÁI SANG PHẢI (CHẠY NGANG)
    TMDB --> INGEST
    INGEST --> MINIO
    INGEST -.-> RAW_LOCAL

    MINIO --> STAGING
    RAW_LOCAL -.-> STAGING

    STAGING --> DIMS
    STAGING --> BMG
    STAGING --> FR

    DIMS --> DQ
    BMG --> DQ
    FR --> DQ

    DQ --> BI
    DQ --> ML

    %% LIÊN KẾT ĐIỀU PHỐI AIRFLOW
    AF_DAG -.->|Điều phối tự động 6 tasks| INGESTLAYER
    AF_DAG -.->|Điều phối tự động 6 tasks| ETLLAYER
    AF_DAG -.->|Điều phối tự động 6 tasks| DWLAYER
    AF_DAG -.->|Điều phối tự động 6 tasks| DQLAYER
```

---

### 2.1. Tóm Tắt Tác Dụng Từng Thành Phần Cốt Lõi trong Kiến Trúc

Nhìn vào sơ đồ kiến trúc, dữ liệu được chuyển hóa qua 7 tầng rõ ràng:

1. **Tầng 1 - Nguồn Dữ Liệu (Data Sources):** Kết hợp dữ liệu đánh giá nội bộ từ MovieLens 100k (`movies`, `ratings`, `links`, `tags`) với siêu dữ liệu phong phú từ The Movie Database (TMDB) API (overview, vote, popularity) để tăng giá trị thông tin.
2. **Tầng 2 - Thu Thập & Hồ Dữ Liệu (Ingestion & Data Lake):** Scripts `ingest_to_minio.py` và `fetch_raw_data.py` nạp toàn bộ snapshot thô vào **MinIO Object Storage** (chuẩn S3 API). Giữ nguyên trạng dữ liệu gốc bất biến (immutable), giúp độc lập hoàn toàn với API bên ngoài và cho phép tái hiện (replay) dữ liệu bất kỳ lúc nào.
3. **Tầng 3 - Điều Phối Tự Động (Apache Airflow):** DAG `movielens_data_warehouse_etl` chạy định kỳ `@daily`, quản lý thứ tự thực thi 6 task tự động từ Ingest, Extract, Transform, Load Dimension, Load Fact đến Validate chất lượng, tự động xử lý retry và giám sát qua Web UI.
4. **Tầng 4 - Xử Lý & Chuẩn Hóa (ETL Engine & Staging):** Vùng đệm `staging/` và module `etl_process.py` (Pandas & SQLAlchemy) đảm nhiệm tiền xử lý quan trọng: khử trùng lặp `(user_id, movie_id)`, đồng nhất grain thời gian theo giờ `%Y%m%d%H`, ghép siêu dữ liệu TMDB, và đặc biệt là sinh bảng cầu nối `bridge_movie_genre` giải quyết triệt để quan hệ Nhiều - Nhiều (N:N).
5. **Tầng 5 - Kho Dữ Liệu OLAP (PostgreSQL Data Warehouse):** Mô hình **Star Schema + Bridge Table** chuẩn OLAP gồm 5 bảng Dimension (`dim_user`, `dim_movie`, `dim_genre`, `bridge_movie_genre`, `dim_time`) và 1 bảng Fact trung tâm (`fact_rating`). Áp dụng cơ chế **Upsert Idempotent (`ON CONFLICT DO UPDATE`)** bảo toàn toàn vẹn khóa chính và khóa ngoại khi pipeline chạy lại.
6. **Tầng 6 - Chốt Chặn Kiểm Định Chất Lượng (Data Quality Gate):** Task `validate_data_warehouse` tự động chặn lỗi: xác minh 0 bảng rỗng, 0 giá trị NULL trên PK, 0 bản ghi mồ côi (toàn vẹn khóa ngoại 100%), và miền giá trị rating hợp lệ $0.5 \le \text{rating} \le 5.0$.
7. **Tầng 7 - Ứng Dụng Hạ Nguồn (Serving Layer):**
   - **Business Intelligence:** Dashboard trực quan hóa trên **Power BI** ([MovieLens_TMDB_BI.pbix](MovieLens_TMDB_BI.pbix)) với Cross-filtering hai chiều qua Bridge Table và các câu lệnh phân tích nâng cao trong [query_dw.sql](query_dw.sql).
   - **Machine Learning & Gợi Ý Phim:** Thuật toán **Hybrid LightFM** kết hợp lọc cộng tác và đặc trưng nội dung (WARP Loss) phục vụ gợi ý cá nhân hóa Top-K ([recommend.py](recommend.py)), kèm engine **Most Popular** giải quyết bài toán Cold-Start.

---

## 3. Báo cáo Hiện trạng & Kết quả Dữ liệu Thực tế (Audit Report)

Hệ thống đã được kiểm tra, xác minh tính toàn vẹn và nạp dữ liệu thực tế thành công qua các tầng lưu trữ:

### 3.1. Dữ liệu thực tế tại Data Lake & Data Warehouse

| Tầng lưu trữ | Thực thể / Bảng | Số bản ghi thực tế | Trạng thái xác minh | Ghi chú kỹ thuật |
|---|---|---:|:---:|---|
| **Data Lake (MinIO)** | `raw-datalake` | 5 objects | Đã nạp | `movies.csv`, `ratings.csv`, `tags.csv`, `links.csv`, `tmdb_movies_raw.json` |
| **Data Warehouse** | `dim_user` | **610** | Đã xác minh | Đầy đủ người dùng từ MovieLens, PK `user_id`. |
| **Data Warehouse** | `dim_movie` | **9.742** | Đã xác minh | 9.724 phim có tương tác rating; 300 phim được làm giàu metadata từ TMDB. |
| **Data Warehouse** | `dim_genre` | **19** | Đã xác minh | 19 thể loại phim chuẩn hóa (Action, Comedy, Drama, v.v.). |
| **Data Warehouse** | `bridge_movie_genre` | **22.050** | Đã xác minh | Giải quyết triệt để quan hệ Nhiều - Nhiều (N:N) giữa phim và thể loại. |
| **Data Warehouse** | `dim_time` | **7.761** | Đã xác minh | Chuẩn hóa theo grain giờ (`%Y%m%d%H`), có timestamp đại diện giờ và đầy đủ cấp ngày/tháng/quý/năm. |
| **Data Warehouse** | `fact_rating` | **100.836** | Đã xác minh | Khử trùng lặp tuyệt đối: 100.836 cặp `(user_id, movie_id)` duy nhất; điểm từ 0.5 đến 5.0. |

### 3.2. Đánh giá chất lượng kỹ thuật & Khắc phục rủi ro ETL

1. **Khắc phục quan hệ N-N Phim — Thể loại:** Bổ sung bảng cầu nối `bridge_movie_genre` giúp Power BI có thể phân tích chính xác doanh số/lượt xem theo từng thể loại mà không làm sai lệch số liệu khi một phim thuộc nhiều thể loại.
2. **Khắc phục Grain của chiều Thời gian:** Grain của `dim_time` được đồng nhất theo cấp độ Giờ (`%Y%m%d%H`), bảo đảm các phép phân tích theo khung giờ trong ngày, thứ trong tuần, tháng, quý và năm luôn nhất quán.
3. **Cơ chế nạp Idempotent an toàn (Thay thế `replace`):** Thay vì sử dụng `if_exists="replace"` (vốn xóa bảng và phá hủy ràng buộc PK/FK), pipeline áp dụng cơ chế **Upsert (`ON CONFLICT DO UPDATE`)**, bảo toàn toàn bộ chỉ mục và ràng buộc toàn vẹn khi chạy lại.
4. **Data Quality Gate tự động:** Tích hợp task kiểm tra tự động trước khi cho phép dữ liệu phục vụ BI và ML:
   - 0 orphan records (không có rating trỏ tới user_id, movie_id hoặc time_id không tồn tại).
   - 0 duplicate pairs trên toàn bộ Primary Keys.
   - Miền giá trị điểm đánh giá đảm bảo $0.5 \le \text{rating} \le 5.0$.
5. **Độ bao phủ dữ liệu TMDB:** Đã gán nhãn trường cờ boolean `has_tmdb_metadata` và giữ giá trị `NULL` cho các phim chưa lấy được điểm TMDB, tránh việc điền giá trị 0 hoặc chuỗi giả định làm lệch phân tích thống kê.

---

## 4. Mô hình Kho Dữ liệu (Star Schema + Bridge Table)

Kho dữ liệu được thiết kế theo kiến trúc **Mô hình hình sao mở rộng (Star Schema with Bridge Table)** chuẩn OLAP:

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

### Chi tiết các thuộc tính chính trong Schema:

| Tên bảng | Loại bảng | Khóa chính (PK) | Khóa ngoại (FK) | Mục đích phân tích |
|---|---|---|---|---|
| **`dim_user`** | Dimension | `user_id` | - | Phân tích thói quen đánh giá, phân khúc người dùng tích cực. |
| **`dim_movie`** | Dimension | `movie_id` | - | Phân tích xu hướng phát hành theo năm/thập niên, độ phổ biến từ TMDB. |
| **`dim_genre`** | Dimension | `genre_id` | - | Danh mục thể loại phim chuẩn hóa phục vụ phân loại lát cắt. |
| **`bridge_movie_genre`** | Bridge | `(movie_id, genre_id)` | `movie_id`, `genre_id` | Cầu nối đa chiều giải quyết quan hệ Nhiều - Nhiều giữa Phim và Thể loại. |
| **`dim_time`** | Dimension | `time_id` | - | Phân tích chu kỳ hoạt động theo giờ trong ngày, ngày trong tuần, quý và năm. |
| **`fact_rating`** | Fact | `(user_id, movie_id)` | `user_id`, `movie_id`, `time_id` | Chứa dữ liệu tương tác cốt lõi: điểm số (`rating`) và thời điểm (`rating_timestamp`). |

---

## 5. Quy trình Vận hành Pipeline ETL tự động qua Airflow

Pipeline ETL được đóng gói và điều phối qua 6 task tuần tự trong DAG `movielens_data_warehouse_etl`:

```mermaid
graph LR
    A["1. ingest_raw_to_minio<br/>(Nạp raw lên Data Lake)"] --> B["2. extract_from_minio<br/>(Trích xuất vào Staging)"]
    B --> C["3. clean_and_transform<br/>(Làm sạch & Chuẩn hóa Schema)"]
    C --> D["4. load_dimension_tables<br/>(Upsert bảng Chiều & Bridge)"]
    D --> E["5. load_fact_rating<br/>(Upsert bảng Sự kiện Fact)"]
    E --> F["6. validate_data_warehouse<br/>(Kiểm định Chất lượng DQ Gate)"]
```

1. **`ingest_raw_to_minio`:** Tự động đẩy snapshot dữ liệu từ thư mục `raw_data/` lên bucket `raw-datalake` của MinIO.
2. **`extract_from_minio`:** Tải các file cần thiết từ MinIO xuống thư mục `staging/` phục vụ xử lý theo batch.
3. **`clean_and_transform`:**
   - Làm sạch tiêu đề phim, trích xuất năm và thập niên phát hành.
   - Ghép mã TMDB qua `links.csv`, nạp metadata TMDB (overview, popularity, vote_average).
   - Tách chuỗi genres dạng văn bản thành các thể loại chuẩn hóa và tạo bảng quan hệ cầu nối `bridge_movie_genre`.
   - Chuẩn hóa timestamp thành `hour_timestamp` và tạo `time_id` dạng `%Y%m%d%H`.
   - Khử trùng lặp `(user_id, movie_id)`, giữ lại tương tác mới nhất.
4. **`load_dimension_tables`:** Khởi tạo cấu trúc bảng nếu chưa có, sau đó thực hiện **Upsert** vào các bảng `dim_user`, `dim_movie`, `dim_genre`, `bridge_movie_genre`, `dim_time`.
5. **`load_fact_rating`:** Upsert dữ liệu vào `fact_rating`, tự động kiểm tra tính toàn vẹn khóa trước khi ghi.
6. **`validate_data_warehouse`:** Chốt chặn kiểm định chất lượng dữ liệu:
   - Đảm bảo tất cả các bảng đều có dữ liệu ($> 0$).
   - Đảm bảo không có giá trị NULL trên các trường khóa chính.
   - Đảm bảo toàn vẹn tham chiếu khóa ngoại: $0$ orphan records.
   - Đảm bảo điểm rating hợp lệ: $0.5 \le \text{rating} \le 5.0$.

---

## 6. Ứng dụng Phân tích Kinh doanh (BI & Analytics)

### 6.1. Báo cáo & Dashboard Power BI
Dự án đã hoàn thiện file dashboard báo cáo chuyên sâu: **[MovieLens_TMDB_BI.pbix](MovieLens_TMDB_BI.pbix)**.

**Hướng dẫn kết nối Power BI với Data Warehouse:**
1. Khởi động **Power BI Desktop** $\rightarrow$ chọn **Get Data** $\rightarrow$ **PostgreSQL database**.
2. **Server:** `localhost:5434` | **Database:** `dw_movies` | Chế độ: **Import**.
3. Nhập tài khoản và mật khẩu đã thiết lập trong file `.env`.
4. Chọn toàn bộ 6 bảng: `dim_user`, `dim_movie`, `dim_genre`, `bridge_movie_genre`, `dim_time`, `fact_rating`.
5. **Cấu hình Model View:**
   - Nối khóa ngoại giữa Fact và các bảng Dimension theo đúng thiết kế ERD ở Mục 4.
   - Đối với quan hệ giữa `dim_movie` và `bridge_movie_genre`: đổi **Cross filter direction** thành **Both** để cho phép lọc Fact qua Thể loại phim.

**Bộ thước đo DAX cốt lõi:**
```dax
Total Ratings = COUNTROWS(fact_rating)
Average Rating = ROUND(AVERAGE(fact_rating[rating]), 2)
Total Movies = DISTINCTCOUNT(dim_movie[movie_id])
Total Active Users = DISTINCTCOUNT(fact_rating[user_id])
```

### 6.2. Truy vấn SQL Mẫu Phân tích (OLAP Queries)
File [query_dw.sql](query_dw.sql) cung cấp sẵn các câu truy vấn mẫu hỗ trợ báo cáo:
- **Top 10 phim được đánh giá cao nhất (có từ 50 lượt đánh giá trở lên):**
  ```sql
  SELECT m.title, m.release_year, ROUND(AVG(f.rating)::numeric, 2) AS avg_rating, COUNT(f.rating) AS total_ratings
  FROM fact_rating f
  JOIN dim_movie m ON f.movie_id = m.movie_id
  GROUP BY m.movie_id, m.title, m.release_year
  HAVING COUNT(f.rating) >= 50
  ORDER BY avg_rating DESC, total_ratings DESC LIMIT 10;
  ```
- **Phân phối lượt đánh giá theo thể loại (sử dụng Bridge Table):**
  ```sql
  SELECT g.genre_name, COUNT(b.movie_id) AS total_movies
  FROM dim_genre g
  JOIN bridge_movie_genre b ON g.genre_id = b.genre_id
  GROUP BY g.genre_id, g.genre_name ORDER BY total_movies DESC;
  ```

---

## 7. Ứng dụng Gợi ý Phim Cá nhân hóa (Recommendation System)

Hệ thống Recommendation được thiết kế trực tiếp trên nguồn dữ liệu sạch của Data Warehouse, kết hợp hài hòa giữa lọc cộng tác (Collaborative Filtering) và đặc trưng nội dung (Content-based Features).

### 7.1. Kiến trúc Thuật toán
- **Mô hình chính:** **Hybrid LightFM** sử dụng hàm mất mát **WARP (Weighted Approximate-Rank Pairwise)**, tối ưu hóa trực tiếp thứ hạng Top-K cho bài toán xếp hạng phim cá nhân hóa.
- **Biểu diễn thực thể:**
  - **User Representation:** User Latent Embeddings học từ ma trận tương tác nhị phân (ngưỡng $\ge 4.0$ sao được coi là positive interaction).
  - **Item Representation:** Kết hợp Identity Embedding với các đặc trưng nội dung (thể loại phim từ MovieLens và các token mô tả nội dung `overview` từ TMDB thông qua Binary CountVectorizer).
- **Mô hình phụ trợ (Baseline / Cold-Start):** **Most Popular Recommender** được sử dụng làm cơ chế fallback cho người dùng mới chưa có lịch sử đánh giá tích cực.

### 7.2. Kết quả Đánh giá Thực nghiệm (So sánh với Baseline)

Đánh giá thực tế trên 608 người dùng kiểm thử (Holdout split 80/20 theo từng user, seed=42):

| Mô hình đề xuất | Precision@10 | Recall@10 | NDCG@10 |
|---|---:|---:|---:|
| **Baseline: Most Popular** | 0.127961 | 0.108645 | 0.171856 |
| **Hybrid LightFM + WARP** | **0.189309** | **0.170421** | **0.247525** |
| *Mức độ cải thiện* | *+47.9%* | *+56.9%* | *+44.0%* |

> Xem tài liệu chi tiết:
> - [RECOMMENDATION_FLOW.md](RECOMMENDATION_FLOW.md): Luồng toán học, kiến trúc đặc trưng và giải thuật chi tiết.
> - [RECOMMENDATION_AUDIT.md](RECOMMENDATION_AUDIT.md): Báo cáo kiểm định thực tế và cẩm nang bảo vệ đồ án.

### 7.3. Hướng dẫn Chạy CLI Gợi ý Phim

Huấn luyện mô hình và chạy thử nghiệm gợi ý phim cho người dùng:
```bash
# Huấn luyện mô hình từ dữ liệu Data Warehouse
python recommend.py --train

# Đánh giá lại các chỉ số chất lượng mô hình
python recommend.py --evaluate

# Chạy gợi ý Top 10 phim cho người dùng có user_id = 15
python recommend.py --user-id 15 --top-k 10
```

---

### 8.1. Thông tin Cổng Dịch vụ & Kết nối Cơ sở Dữ liệu

Toàn bộ thông số kết nối đã được thiết lập đầy đủ trong file [`.env`](.env):

| Dịch vụ | Địa chỉ Host | Cổng (Port) | Tài khoản (User) | Mật khẩu (Password) | Database / Bucket | Ghi chú & Mục đích |
|---|---|---:|---|---|---|---|
| **PostgreSQL Data Warehouse** | `localhost` | **`5434`** | `dw_user` | `dw_password` | `dw_movies` | Kho dữ liệu OLAP (Power BI, DBeaver, Python) |
| **MinIO Web Console** | `http://localhost:9001` | **`9001`** | `minioadmin` | `minioadmin` | - | Giao diện Web quản trị Data Lake |
| **MinIO S3 API Endpoint** | `localhost:9000` | **`9000`** | `minioadmin` | `minioadmin` | `raw-datalake` | Endpoint S3 API nạp/xuất dữ liệu thô |
| **Airflow Web UI** | `http://localhost:8080` | **`8080`** | `admin` | `admin` | - | Giao diện điều phối & kích hoạt DAG |
| **Airflow Metadata DB** | `airflow-db` (nội bộ) | `5432` | `airflow` | `airflow_password` | `airflow` | CSDL lưu trạng thái nội bộ của Airflow |

#### Chuỗi kết nối chuẩn (Connection Strings):
- **Python / SQLAlchemy (dùng cho script ngoài Docker hoặc máy host Windows):**
  ```text
  postgresql+psycopg2://dw_user:dw_password@localhost:5434/dw_movies
  ```
- **Java / JDBC (dùng cho DBeaver, DataGrip, Navicat, v.v.):**
  ```text
  jdbc:postgresql://localhost:5434/dw_movies
  ```
- **Lệnh kết nối nhanh qua dòng lệnh (psql qua Docker container):**
  ```powershell
  docker compose exec postgres-dw psql -U dw_user -d dw_movies
  ```

#### Hướng dẫn kết nối từ các công cụ phổ biến:
1. **Power BI Desktop:**
   - Chọn **Get Data** $\rightarrow$ **PostgreSQL database**.
   - **Server:** `localhost:5434` | **Database:** `dw_movies` | **Data Connectivity mode:** `Import`.
   - **User:** `dw_user` | **Password:** `dw_password`.
2. **DBeaver / DataGrip / pgAdmin:**
   - **Host:** `localhost` | **Port:** `5434` | **Database:** `dw_movies`.
   - **Username:** `dw_user` | **Password:** `dw_password`.
3. **Môi trường nội bộ giữa các container trong Docker:**
   - Host nội bộ: `postgres-dw` | Port nội bộ: `5432` (URL: `postgresql+psycopg2://dw_user:dw_password@postgres-dw:5432/dw_movies`).

### 8.2. Hướng dẫn Khởi chạy Hệ thống

#### Bước 1: Thiết lập biến môi trường
Kiểm tra hoặc tùy chỉnh các biến môi trường và mật khẩu trong file `.env`:
```powershell
notepad .env
```

#### Bước 2: Khởi động toàn bộ cụm dịch vụ qua Docker
Chạy script tự động hóa khởi động Airflow và hạ tầng (PowerShell trên Windows):
```powershell
.\start_airflow.ps1
```
Hoặc khởi động thủ công bằng Docker Compose:
```bash
docker compose up -d
```

#### Bước 3: Kích hoạt Pipeline trên Airflow
1. Truy cập giao diện Airflow Web UI tại: [http://localhost:8080](http://localhost:8080).
2. Đăng nhập bằng tài khoản quản trị đã khai báo trong file `.env`.
3. Bật kích hoạt DAG `movielens_data_warehouse_etl` và nhấn **Trigger DAG** để chạy pipeline toàn trình.

#### Bước 4: Kiểm tra trạng thái dữ liệu
Sau khi DAG hoàn tất, bạn có thể kiểm tra dữ liệu bằng script kiểm tra hoặc kết nối Power BI vào cổng `5434` để xem trực quan hóa ngay lập tức.
