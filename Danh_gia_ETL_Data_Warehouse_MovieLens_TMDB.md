# ĐÁNH GIÁ HIỆN TRẠNG ETL — BÀI TẬP LỚN KHO DỮ LIỆU VÀ KHAI PHÁ DỮ LIỆU

**Đề tài:** Xây dựng kho dữ liệu đa nguồn phục vụ phân tích hành vi người dùng và gợi ý phim cá nhân hóa  
**Repository:** https://github.com/Aizen2510/DATTA_CUOIKI  
**Thời điểm tổng hợp:** 18/10/2026  
**Phạm vi:** Tổng hợp kết quả đã kiểm tra qua PostgreSQL, MinIO, Docker và nội dung mã nguồn ETL/DAG đã trao đổi. Không phải báo cáo kiểm thử end-to-end hoàn chỉnh.

## 1. Kết luận ngắn

**ETL không phải đang hỏng.** Hệ thống đã tạo được dữ liệu thực tế trong Data Lake (MinIO) và Data Warehouse (PostgreSQL). Tuy vậy, thiết kế kho dữ liệu và quy trình vận hành còn các điểm cần hoàn thiện trước khi coi pipeline đạt mức hoàn chỉnh. **Người phụ trách BI có thể bắt đầu ngay** với các dashboard tổng quan, rating và hành vi người dùng; một số phân tích chuyên sâu phải chờ thống nhất mô hình.

## 2. Tiến độ theo luồng kiến trúc của giảng viên

| Công đoạn | Kết quả đã ghi nhận | Đánh giá |
|---|---|---|
| Data Sources | MovieLens và dữ liệu bổ sung TMDB | Đã có dữ liệu từ hai nguồn |
| Ingestion | Có các object MovieLens/TMDB trong MinIO | Có kết quả nạp; chưa xác minh tự động hóa đầu-cuối |
| Data Lake | Bucket `raw-datalake`, 5 objects | Đã xác minh |
| ETL (Python) | Đã tạo các bảng và dữ liệu trong PostgreSQL | Có đầu ra thực; chưa kiểm thử chạy lại toàn bộ pipeline |
| Data Warehouse | 5 bảng Dimension/Fact, có dữ liệu | Đã xác minh ở mức dữ liệu và các kiểm tra cơ bản |
| Airflow | Có file DAG trong repository | Cụm Airflow đang chạy trên máy thuộc thư mục bài thực hành khác, chưa nhận DAG bài tập lớn |
| BI/OLAP | Chưa có sản phẩm dashboard được xác minh | Có thể bắt đầu |
| Data Mining / ML | Chưa có kết quả thuật toán được xác minh | Còn cần thực hiện |
| Gợi ý phim cá nhân hóa | Chưa có sản phẩm đã được xác minh | Còn cần thực hiện |

## 3. Bằng chứng dữ liệu thực tế

### 3.1. Data Lake — MinIO

- Bucket: `raw-datalake` (Private).
- Ghi nhận 5 objects, tổng kích thước khoảng 3,3 MiB.
- Nhóm MovieLens: `movies.csv`, `ratings.csv`, `tags.csv`, `links.csv`.
- Nhóm TMDB: `tmdb_movies_raw.json`.

### 3.2. Data Warehouse — PostgreSQL

| Bảng | Số bản ghi đã quan sát |
|---|---:|
| `dim_user` | 610 |
| `dim_movie` | 9.742 |
| `dim_time` | 7.761 |
| `dim_genre` | 19 |
| `fact_rating` | 100.836 |

Các phép kiểm tra đã thực hiện:
- `missing_movies = 0`, `missing_users = 0` khi nối `fact_rating` tới `dim_movie` và `dim_user`.
- Số cặp `(user_id, movie_id)` duy nhất bằng số hàng `fact_rating`: 100.836, tức không có cặp trùng theo tiêu chí này.
- Có 9.734 phim chứa `tmdb_id`, nhưng **có ID TMDB không đồng nghĩa metadata API đã được lấy thành công**.
- `overview` có nội dung không rỗng ở 9.742 phim; cần lưu ý giá trị thay thế như `No overview available` không phải mô tả thực từ TMDB.
- `popularity > 0`: 300 phim; `vote_average > 0`: 300 phim. Đây là bằng chứng metadata có giá trị dương mới bao phủ một phần nhỏ tập phim; chưa đủ để kết luận chính xác số phim fetch thành công nếu chưa xem dữ liệu thô và giá trị mặc định.

## 4. Những vấn đề cần khắc phục trong ETL và mô hình kho dữ liệu

### 4.1. `dim_genre` chưa có quan hệ rõ ràng với phim — ưu tiên cao đối với BI

**Quan sát:** Có 19 thể loại trong `dim_genre`, trong khi `dim_movie` lưu `genres` dạng văn bản; chưa thấy bảng nối chuẩn hóa giữa phim và thể loại. `fact_rating` cũng không chứa `genre_id`.

**Hệ quả:** Khó dùng `dim_genre` làm chiều lọc/phân tích đáng tin cậy trong Star Schema/Power BI. Một phim có thể thuộc nhiều thể loại nên cần mô hình quan hệ nhiều–nhiều.

**Đề xuất:** Bổ sung bảng `bridge_movie_genre(movie_id, genre_id)` có cặp khóa duy nhất, khóa ngoại phù hợp và kiểm tra các thể loại không khớp. Khi phân tích theo thể loại, phải giải thích việc một lượt đánh giá có thể được tính vào nhiều thể loại; tổng các thể loại có thể lớn hơn tổng rating.

### 4.2. `dim_time` chưa nhất quán độ chi tiết — ưu tiên cao đối với BI theo thời gian

**Theo code đã xem:** `time_id` được tạo từ `rating_time` theo định dạng `%Y%m%d%H` (độ chi tiết theo giờ); bảng `dim_time` giữ `rating_timestamp` có thể chi tiết đến giây rồi loại trùng theo `time_id`.

**Hệ quả:** Nhiều lượt đánh giá cùng giờ chia sẻ một khóa thời gian, nhưng timestamp đại diện trong dimension có thể là thời điểm của chỉ một lượt đánh giá. Điều này không phù hợp nếu trực quan hóa đến phút/giây hoặc hiểu sai ý nghĩa trường thời gian.

**Đề xuất:** Thống nhất grain của `dim_time` là theo ngày/giờ; lưu mốc giờ đã chuẩn hóa hoặc giữ timestamp gốc ở Fact nếu cần phân tích độ chi tiết cao hơn. Kiểm tra tính duy nhất của `time_id` và độ chính xác phép nối.

### 4.3. Dùng `to_sql(if_exists="replace")` — ưu tiên cao về độ an toàn khi nạp

**Theo code đã xem:** Một số bảng được Pandas ghi bằng `if_exists="replace"`.

**Hệ quả:** Bảng cũ có thể bị xóa và tái tạo; mất các khóa/chỉ mục tạo thủ công, gây rủi ro nếu pipeline thất bại giữa các lần nạp. Không phù hợp cho cập nhật tăng dần nếu không có cơ chế khác.

**Đề xuất:** Với dữ liệu tĩnh, có thể chấp nhận full refresh nhưng nên nạp qua staging, kiểm tra chất lượng và chuyển đổi dữ liệu có kiểm soát/transaction. Nếu mục tiêu là cập nhật liên tục, cần thiết kế incremental load hoặc upsert phù hợp.

### 4.4. Ràng buộc Primary Key / Foreign Key — cần hoàn thiện

**Quan sát:** Khi kiểm tra mô tả một số bảng bằng `\d`, chưa thấy các ràng buộc PK/FK tương ứng được khai báo. Kết quả JOIN hiện tại tốt nhưng chưa thay thế cơ chế bảo vệ toàn vẹn khi nạp sau này.

**Đề xuất:** Xác định PK và FK cho các Dimension/Fact/bridge, kiểm tra duplicate trước khi tạo ràng buộc, bổ sung index cần thiết. Không tự ý thêm khóa trong database hiện tại khi chưa sao lưu và thống nhất với nhóm.

### 4.5. Metadata TMDB mới bao phủ một phần — cần hoàn thiện cho phân tích nội dung

**Quan sát:** Chỉ 300/9.742 phim có `popularity > 0` và `vote_average > 0`. Việc có `tmdb_id` và `overview` không rỗng có thể xuất phát từ mapping ID và các chuỗi mặc định.

**Hệ quả:** Các dashboard theo `popularity`, `vote_average` hay mô tả TMDB sẽ thiên lệch nếu trình bày như dữ liệu đại diện cho toàn bộ phim.

**Đề xuất:** Kiểm tra số bản ghi thực sự fetch thành công, tỷ lệ ghép MovieLens–TMDB, phân biệt giá trị thiếu với 0; mở rộng ingestion theo giới hạn API và quy định sử dụng nếu cần.

### 4.6. Data Quality tự động — cần bổ sung

Đã có các phép kiểm tra thủ công cho count, thiếu phim/người dùng và cặp user–movie trùng, nhưng chưa thấy bằng chứng một lớp kiểm tra tự động đầy đủ trong pipeline.

Nên có các kiểm tra: số lượng trước/sau biến đổi; null và duplicate ở khóa; miền giá trị rating; tính toàn vẹn tham chiếu; thống kê tỷ lệ dữ liệu TMDB có thực; báo lỗi khi chỉ tiêu không đạt và ghi log theo từng lần chạy.

### 4.7. Airflow chưa điều phối DAG bài tập lớn — vấn đề triển khai, không phải bằng chứng lỗi ETL

- DAG trong repo: `dags/movielens_etl_dag.py`, ID `movielens_data_warehouse_etl`.
- DAG định nghĩa: `extract_from_minio` → `clean_and_transform` → `load_dimension_tables` → `load_fact_rating`.
- Cụm Docker Airflow 3.3.2 trên máy đang mount từ thư mục `data-warehouse-lab/airflow/dags` của bài thực hành khác và chỉ thấy `customer_etl.py` trong thư mục DAG riêng đó.
- Vì vậy không có căn cứ khẳng định DAG của bài tập lớn đã chạy thành công qua Airflow.
- Cần kiểm tra thêm việc các hàm ETL nhận/truyền dữ liệu qua task như thế nào: dấu `>>` chỉ định thứ tự thực hiện, không tự động chuyển đối tượng DataFrame giữa các task.

**Đề xuất:** Kiểm thử DAG trong môi trường tách biệt, đảm bảo thư viện, đường dẫn, kết nối mạng/biến môi trường, cách lưu kết quả trung gian, và kiểm tra an toàn trước khi bật lịch chạy.

### 4.8. Thông tin kết nối / cấu hình — cần cải thiện

Theo phiên bản code đã trao đổi, có dấu hiệu sử dụng kết nối `localhost` và thông tin đăng nhập cấu hình cố định. Điều này có thể hoạt động khi chạy Python trên máy host nhưng không nhất thiết đúng khi ETL chạy bên trong container. Không đưa khóa API/mật khẩu vào Git; ưu tiên biến môi trường hoặc công cụ quản lý secrets và thay thông tin mặc định đang được sử dụng.

## 5. Mức độ ảnh hưởng đến công việc BI

| Phân tích BI | Có thể triển khai ngay? | Điều kiện / lưu ý |
|---|---|---|
| KPI tổng phim, người dùng, rating | **Có** | Dùng `dim_movie`, `dim_user`, `fact_rating` |
| Top phim nhiều lượt rating / rating trung bình | **Có** | JOIN `fact_rating.movie_id = dim_movie.movie_id`; đặt ngưỡng số rating khi xếp hạng điểm |
| Phân phối điểm rating và mức độ hoạt động người dùng | **Có** | Dùng Fact và `dim_user` |
| Phân tích theo thể loại | **Có điều kiện** | Cần quan hệ phim–thể loại thống nhất, ưu tiên bridge |
| Xu hướng rating theo thời gian | **Có điều kiện** | Kiểm tra grain và dữ liệu `dim_time` |
| Phân tích theo độ phổ biến/vote TMDB | **Có điều kiện** | Chỉ báo cáo trên tập có metadata hợp lệ, nêu rõ mức độ bao phủ |
| Chất lượng gợi ý phim | **Chưa** | Cần output/metrics từ nhánh ML |

**Phần BI có thể bắt đầu ngay** bằng cách kết nối Power BI Desktop với PostgreSQL trên Windows: `localhost:5434`, database `dw_movies`, user `dw_user` (mật khẩu lấy từ cấu hình nội bộ của nhóm). Các quan hệ trong Power BI có thể tạo theo ID ngay cả khi PostgreSQL chưa khai báo FK; nhưng cần bảo đảm phía dimension duy nhất và đúng grain.

## 6. Danh sách công việc đề xuất cho nhóm, theo thứ tự ưu tiên

1. **Chốt mô hình phim–thể loại:** tạo/chuẩn hóa `bridge_movie_genre`, nhất quán mapping ID và hướng lọc trong BI.
2. **Chốt grain thời gian:** điều chỉnh `dim_time`, bảo đảm phân tích theo giờ/ngày không sai nghĩa.
3. **An toàn quy trình nạp:** xác định full refresh hay incremental; thay `replace` trực tiếp bằng phương pháp được kiểm soát; bổ sung PK/FK và index hợp lý.
4. **Kiểm tra chất lượng ETL tự động:** kiểm tra count, null, uniqueness, referential integrity và coverage của TMDB.
5. **Bổ sung metadata TMDB:** đo số phim thật sự được làm giàu; mở rộng nếu mục tiêu bài toán cần.
6. **Xác minh Airflow:** tích hợp DAG bài tập lớn vào môi trường riêng, kiểm tra import, dependencies, task outputs và lần chạy thành công mà không phá dữ liệu hiện có.
7. **Triển khai BI song song:** làm dashboard Tổng quan + Hành vi người dùng trước, sau đó thể loại và thời gian khi mô hình đã chốt.
8. **Thực hiện nhánh Data Mining/Recommendation:** thống nhất bảng dữ liệu đầu vào, cách chia train/test và chỉ số đánh giá, cung cấp kết quả cho dashboard ML về sau.

## 7. Những điều chưa nên kết luận

- Chưa chứng minh một lần chạy ETL đầu-cuối mới đã thành công; dữ liệu hiện có chứng minh **đầu ra từng được nạp**.
- Chưa chứng minh DAG của bài tập lớn chạy thành công trong Airflow.
- Chưa xác nhận tất cả 9.742 mô tả phim đến từ TMDB, vì có khả năng dùng chuỗi thay thế.
- Chưa xác minh các ràng buộc của toàn bộ 5 bảng ở mức DDL đầy đủ.
- File `etl_process.py` trên máy người dùng có **5 dòng thêm vào chưa commit**; đánh giá mã nguồn trước đó dựa trên phiên bản công khai, vì vậy cần xem `git diff` trước khi sửa/triển khai.

## 8. Kết luận cuối

**Kho dữ liệu đã có đầu ra thực, đủ cho BI cơ bản; ETL cần hoàn thiện mô hình quan hệ, thời gian, cơ chế nạp, kiểm tra chất lượng, metadata TMDB và Airflow.** Thành viên BI không cần chờ toàn bộ những việc này mới bắt đầu, nhưng cần phối hợp với nhóm để chốt bảng thể loại, thời gian và các khóa trước khi chốt dashboard cuối cùng.
