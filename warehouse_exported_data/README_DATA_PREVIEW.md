# DỮ LIỆU ĐÃ NẠP TRONG DATA WAREHOUSE & HƯỚNG DẪN POWER BI

Toàn bộ dữ liệu sau khi được trích xuất từ Data Lake (MinIO), làm sạch và chuẩn hóa qua Apache Airflow **đã được nạp thành công 100% vào Data Warehouse PostgreSQL** (lưu trữ trong volume `dw_data/`).

Thư mục này chứa bản trích xuất nguyên vẹn 6 bảng dữ liệu từ Data Warehouse sang định dạng CSV chuẩn, giúp bạn mở xem trực tiếp hoặc nạp ngay vào Power BI Desktop trên Windows.

---

## 1. Danh mục các file dữ liệu trong Warehouse

| Tên File CSV | Số dòng dữ liệu | Dung lượng | Mô tả chi tiết |
|---|---:|---:|---|
| **[dim_user.csv](dim_user.csv)** | 610 dòng | ~3 KB | Danh sách tài khoản người dùng duy nhất |
| **[dim_movie.csv](dim_movie.csv)** | 9.742 dòng | ~1.6 MB | Phim, năm, thể loại gộp, độ phổ biến, điểm TMDB |
| **[dim_genre.csv](dim_genre.csv)** | 19 dòng | ~1 KB | 19 thể loại phim chuẩn hóa |
| **[bridge_movie_genre.csv](bridge_movie_genre.csv)** | 22.050 dòng | ~320 KB | Bảng cầu nối quan hệ N-N giữa Phim và Thể loại |
| **[dim_time.csv](dim_time.csv)** | 7.761 dòng | ~450 KB | Bảng thời gian chuẩn hóa theo khung giờ |
| **[fact_rating.csv](fact_rating.csv)** | 100.836 dòng | ~3.8 MB | Điểm số đánh giá (`rating`) và thời gian chi tiết |

---

## 2. Mẫu dữ liệu thực tế trích xuất từ Warehouse (Data Preview)

### 2.1. Bảng `dim_movie` (Thông tin phim & TMDB metadata)
| movie_id | title | release_year | genres | popularity | vote_average | has_tmdb_metadata |
|---:|---|---:|---|---:|---:|:---:|
| 1 | Toy Story | 1995 | Adventure, Animation, Children, Comedy, Fantasy | 37.50 | 7.99 | True |
| 2 | Jumanji | 1995 | Adventure, Children, Fantasy | 19.38 | 7.24 | True |
| 3 | Grumpier Old Men | 1995 | Comedy, Romance | 7.55 | 6.50 | True |
| 4 | Waiting to Exhale | 1995 | Comedy, Drama, Romance | 4.88 | 6.20 | True |
| 5 | Father of the Bride Part II | 1995 | Comedy | 8.39 | 6.20 | True |

### 2.2. Bảng `bridge_movie_genre` (Cầu nối quan hệ Phim - Thể loại)
| movie_id | genre_id | Ý nghĩa quan hệ chuẩn hóa |
|---:|---:|---|
| 1 | 2 | Phim Toy Story (ID: 1) thuộc thể loại Adventure (ID: 2) |
| 1 | 3 | Phim Toy Story (ID: 1) thuộc thể loại Animation (ID: 3) |
| 1 | 4 | Phim Toy Story (ID: 1) thuộc thể loại Children (ID: 4) |
| 1 | 5 | Phim Toy Story (ID: 1) thuộc thể loại Comedy (ID: 5) |
| 1 | 9 | Phim Toy Story (ID: 1) thuộc thể loại Fantasy (ID: 9) |

### 2.3. Bảng `fact_rating` (Lượt đánh giá người dùng)
| user_id | movie_id | time_id | rating | rating_timestamp |
|---:|---:|---:|---:|---|
| 1 | 1 | 2000073018 | 4.0 | 2000-07-30 18:45:03 |
| 1 | 3 | 2000073018 | 4.0 | 2000-07-30 18:20:47 |
| 1 | 6 | 2000073018 | 4.0 | 2000-07-30 18:37:04 |
| 1 | 47 | 2000073019 | 5.0 | 2000-07-30 19:03:35 |
| 1 | 50 | 2000073018 | 5.0 | 2000-07-30 18:48:51 |

---

## 3. HƯỚNG DẪN KẾT NỐI POWER BI DESKTOP (WINDOWS)

Bạn có thể đưa dữ liệu vào Power BI Desktop trên Windows theo **1 trong 2 cách** sau:

### CÁCH 1: Kết nối trực tiếp vào PostgreSQL Data Warehouse (Khuyên dùng)

1. **Khởi động Power BI Desktop trên Windows**.
2. Trên thanh Ribbon, bấm **Get Data** (Lấy dữ liệu) → chọn **PostgreSQL database** → bấm **Connect**.
3. **Nhập thông tin kết nối:**
   - **Server:** `localhost:5434` *(nếu Power BI chạy cùng máy với Docker)* hoặc `<IP_máy_chủ>:5434` *(nếu máy khác trong mạng LAN)*.
   - **Database:** `dw_movies`
   - **Data Connectivity mode:** Chọn **Import** → bấm **OK**.
4. **Xác thực tài khoản (Authentication):**
   - Chọn mục **Database** ở cột bên trái.
   - **User name:** `dw_user`
   - **Password:** `dw_password`
   - Bấm **Connect**.
5. **Chọn dữ liệu:** Tích chọn toàn bộ 6 bảng (`dim_user`, `dim_movie`, `dim_genre`, `bridge_movie_genre`, `dim_time`, `fact_rating`) → bấm **Load**.

---

### CÁCH 2: Nạp dữ liệu từ các file CSV trong thư mục này

Nếu máy Windows không kết nối mạng được tới PostgreSQL, bạn chỉ cần copy thư mục `warehouse_exported_data/` sang máy Windows:
1. Mở **Power BI Desktop** → **Get Data** → chọn **Text/CSV**.
2. Lần lượt nạp 6 file: `dim_user.csv`, `dim_movie.csv`, `dim_genre.csv`, `bridge_movie_genre.csv`, `dim_time.csv`, `fact_rating.csv`.

---

## 4. Thiết lập Mô hình Quan hệ (Model View) trong Power BI

Sau khi nạp dữ liệu vào Power BI Desktop, bạn chuyển sang tab **Model view** (biểu tượng sơ đồ khối ở thanh bên trái) và kiểm tra các đường nối quan hệ `1 - Nhiều (1 - *)`:

- `dim_user (user_id)` 1 ─── * `fact_rating (user_id)`
- `dim_movie (movie_id)` 1 ─── * `fact_rating (movie_id)`
- `dim_time (time_id)` 1 ─── * `fact_rating (time_id)`
- `dim_movie (movie_id)` 1 ─── * `bridge_movie_genre (movie_id)`
- `dim_genre (genre_id)` 1 ─── * `bridge_movie_genre (genre_id)`

> **LƯU Ý CỰC KỲ QUAN TRỌNG VỚI BẢNG BRIDGE:**
> - Click đúp vào đường nối giữa `dim_movie` và `bridge_movie_genre`.
> - Tại ô **Cross filter direction** (Hướng lọc chéo), chọn **Both** (Hai chiều) → bấm **OK**.
> - **Ý nghĩa:** Khi chọn 1 thể loại ở `dim_genre`, bộ lọc sẽ tự động truyền qua `dim_movie` và lọc chính xác các đánh giá trong `fact_rating`.

---

## 5. Tạo các DAX Measures quan trọng (Thước đo tính toán)

Tại tab **Report view**, bấm **New Measure** trên thanh công cụ và dán lần lượt các công thức DAX sau:

```dax
// 1. Tổng số lượt đánh giá
Total Ratings = COUNTROWS(fact_rating)

// 2. Điểm đánh giá trung bình
Average Rating = ROUND(AVERAGE(fact_rating[rating]), 2)

// 3. Tổng số bộ phim trong kho
Total Movies = DISTINCTCOUNT(dim_movie[movie_id])

// 4. Tổng số người dùng tích cực
Total Active Users = DISTINCTCOUNT(fact_rating[user_id])
```

---

## 6. Gợi ý thiết kế 3 trang Dashboard mẫu nộp bài

### 📄 Trang 1: Tổng quan Kho Dữ liệu (Overview Dashboard)
- **Hàng thẻ KPI (Card Visuals):**
  - Thẻ 1: `Total Ratings` (100.8K lượt)
  - Thẻ 2: `Average Rating` (3.50 ⭐)
  - Thẻ 3: `Total Movies` (9.74K bộ phim)
  - Thẻ 4: `Total Active Users` (610 người dùng)
- **Biểu đồ cột (Clustered Column Chart):** Phân bố điểm số rating
  - *Trục X:* `fact_rating[rating]` (từ 0.5 đến 5.0)
  - *Trục Y:* `[Total Ratings]`
- **Biểu đồ tròn (Donut Chart):** Tỷ lệ phim theo thập niên phát hành
  - *Legend:* `dim_movie[release_decade]`
  - *Values:* `[Total Movies]`

### 📄 Trang 2: Phân tích Thể loại & Xếp hạng Phim (Genre & Movie Analytics)
- **Bộ lọc (Slicer):** `dim_genre[genre_name]` (chọn thể loại: Action, Drama, Comedy, v.v.).
- **Bảng Top 10 phim điểm cao nhất (Table / Matrix):**
  - *Columns:* `dim_movie[title]`, `dim_movie[release_year]`, `[Average Rating]`, `[Total Ratings]`
  - *Visual Filter:* Lọc `[Total Ratings] >= 50` (tránh phim chỉ có 1 vote 5 sao lên top).
- **Biểu đồ thanh ngang (Clustered Bar Chart):** Top thể loại có nhiều phim nhất
  - *Trục Y:* `dim_genre[genre_name]`
  - *Trục X:* `COUNT(bridge_movie_genre[movie_id])`

### 📄 Trang 3: Xu hướng Đánh giá theo Thời gian (Time Trend)
- **Biểu đồ đường (Line Chart):** Xu hướng số lượng rating qua các năm
  - *Trục X:* `dim_time[year]`
  - *Trục Y:* `[Total Ratings]`
- **Biểu đồ cột theo ngày trong tuần:** Thói quen xem/đánh giá phim của người dùng
  - *Trục X:* `dim_time[day_name]` (Monday → Sunday)
  - *Trục Y:* `[Total Ratings]`
