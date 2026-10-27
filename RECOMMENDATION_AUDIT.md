# Recommendation System — kết quả đã chạy thực tế

Ngày kiểm tra: 26/10/2026. Phạm vi: LightFM + WARP, baseline Most Popular, CLI. Báo cáo này thay thế kết luận tạm dừng ban đầu: sau đó người dùng đã cho phép khởi động và chạy pipeline hiện có để chuẩn bị dữ liệu.

## A. Kiểm tra điều kiện đầu vào

### Thu thập và Raw Storage: đạt với phương án local

Có `raw_data/movies.csv` (9.742 dòng), `ratings.csv` (100.836), `links.csv` (9.742), `tags.csv` (3.683) và `tmdb_movies_raw.json` (300 bản ghi). Không gọi TMDB API mới. Chỉ xác minh nội dung JSON lưu sẵn, không có log HTTP chứng minh lại từng request thu thập.

MinIO không chạy được: registry `quay.io/minio/minio:latest` trả HTTP 401, Docker Hub không tải được image thay thế, endpoint tải binary trả HTTP 410. Vì đề bài chấp nhận Raw Storage local, đã dùng thư mục raw có sẵn; không tuyên bố MinIO hoạt động.

Luồng thực tế đã chạy:

```text
MovieLens CSV + TMDB JSON có sẵn
  → raw_data/ (Raw Storage local)
  → Python ETL → staging riêng trong ml_artifacts/
  → PostgreSQL Data Warehouse
  → recommend.py (transaction READ ONLY)
  → train/evaluate → model và CLI
```

### ETL: đạt, chạy mới thành công

Chạy `etl_process.py` với `RAW_STORAGE=local`, exit code 0. Giữ nguyên transform, DDL và upsert hiện có. Chỉ thêm tùy chọn đọc nguồn local rõ ràng; mặc định vẫn MinIO. Không sử dụng pickle staging cũ làm nguồn ETL: staging mới được sinh tại `ml_artifacts/staging/`.

Tích hợp `movies.movieId = links.movieId`, sau đó `links.tmdbId = TMDB.tmdb_id`. Kiểm tra trực tiếp bản đã nạp: 0 sai lệch TMDB ID sau chuẩn hóa số; overview của cả 300 phim được làm giàu khớp JSON raw. Ratings được khử trùng theo (userId, movieId), giữ timestamp mới nhất. Có làm sạch title, chuẩn hóa thời gian theo giờ và bridge thể loại.

Bằng chứng: `ml_artifacts/etl.log`, `ml_artifacts/warehouse_verification.json`.

### Data Warehouse: đạt, xác minh trực tiếp

PostgreSQL 15.19, database `dw_movies`, cổng host 5434. Đây là database local mới được nạp qua ETL hiện có, không phải khôi phục nguyên volume Warehouse cũ.

| Bảng | Số dòng truy vấn thực tế |
|---|---:|
| dim_user | 610 |
| dim_movie | 9.742 |
| dim_genre | 19 |
| bridge_movie_genre | 22.050 |
| dim_time | 7.761 |
| fact_rating | 100.836 |

Đã kiểm tra information_schema và JOIN trực tiếp. Cả 6 bảng có PK; fact có 3 FK, bridge có 2 FK. Không trùng cặp user–movie, không orphan user/movie/time/bridge, rating trong [0.5, 5]. ETL validate thành công. Đây là chạy Python trực tiếp; không tuyên bố một DAG Airflow mới đã chạy.

Kiểu cột xác minh từ database:

| Cột | Kiểu PostgreSQL |
|---|---|
| fact_rating.user_id, movie_id | integer |
| fact_rating.rating | double precision |
| fact_rating.time_id | bigint |
| fact_rating.rating_timestamp | timestamp without time zone |
| dim_movie.movie_id, tmdb_id | integer |
| dim_movie.title, genres, overview | text |
| dim_movie.has_tmdb_metadata | boolean |

Các trường tương ứng userId/movieId/movie_title trong yêu cầu là user_id/movie_id/title trong Warehouse thực tế. Không có tags/keywords trong schema, nên không đọc tags raw để đưa vào model.

### Dữ liệu ML thực tế

610 user, 9.742 phim catalog (9.724 có rating), 100.836 ratings, 48.580 positive (rating >=4). 300 phim có TMDB metadata thật, khoảng 3,08% catalog; còn lại không dùng overview placeholder làm feature. 34 phim không có thể loại cụ thể.

Trích xuất qua JOIN fact_rating → dim_movie/dim_user trong transaction REPEATABLE READ + READ ONLY. Không đọc CSV raw hoặc CSV export để train. Kiểm tra đối chiếu nguồn bằng CSV/JSON chỉ phục vụ audit ETL.

Đã kiểm tra RAM host trước khi chạy (15.701 MiB tổng, 6.584 MiB available). Code dùng sparse matrices và tính score từng user, không lấy mẫu dataset hoặc tạo ma trận điểm dense toàn bộ user. Có ước lượng dung lượng trước tải, giới hạn mặc định 1.024 MiB qua ML_MEMORY_BUDGET_MB; đây là ước lượng bảo vệ, không phải phép đo peak RAM.

## B. Các file đã tạo hoặc chỉnh sửa

| File | Chức năng/lý do |
|---|---|
| recommend.py | Một script đọc Warehouse, chia dữ liệu, tạo features, train, đánh giá, lưu model, demo Top-N và fallback |
| requirements-ml.txt | Dependencies môi trường Python 3.11 riêng; không thay requirements Airflow |
| etl_process.py | Thêm RAW_STORAGE=local cho bước extract vì MinIO không khả dụng; giữ mặc định minio và phần transform/load |
| .gitignore | Bỏ qua ml_artifacts/ chứa model, staging, log |
| README.md | Thêm liên kết báo cáo và lệnh demo để dễ tìm phần Recommendation |
| .env | Cấu hình riêng, mật khẩu sinh ngẫu nhiên, quyền 600, không đưa vào Git |
| RECOMMENDATION_AUDIT.md | Báo cáo hiện tại, hướng dẫn và giải thích bảo vệ |

Artifact local (không commit): `ml_artifacts/model.pkl`, `metrics.json`, `verification.json`, `warehouse_verification.json`, `etl.log`, `training.log`, `dependencies.log`, `environment.txt` và staging. Pickle chứa snapshot dữ liệu đọc từ Warehouse, features, mapping, model evaluation và model demo refit. Chỉ load pickle tự tạo, không dùng artifact không rõ nguồn. Demo dùng lịch sử tại thời điểm train; cần train lại sau khi Warehouse cập nhật.

## C. Cách chạy

### Trên workspace đã chuẩn bị

```bash
source .venv/bin/activate
docker compose up -d --wait postgres-dw
uv run --active --no-project python recommend.py --train
uv run --active --no-project python recommend.py --evaluate
uv run --active --no-project python recommend.py --user-id 15 --top-k 10
```

`--train` tự đọc Warehouse, chia tập, huấn luyện model đánh giá, tính cả hai baseline/model, sau đó refit model demo trên toàn bộ positive và lưu artifact. Không dùng model refit để tính metric test. Model đánh giá và model demo dùng cùng feature vocabulary fit từ train để giữ cách xử lý đơn giản, tránh rò rỉ test.

Khởi động lại PostgreSQL nếu đã tắt:

```bash
docker compose up -d --wait postgres-dw
```

Khi database đã có dữ liệu, không cần chạy lại ETL để demo hoặc retrain. `.env` được recommend.py tự nạp, biến môi trường bên ngoài được ưu tiên. Không hardcode mật khẩu.

### Cài mới môi trường ML (Linux, Python 3.11)

Nếu đã có Python 3.11:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install 'pip==24.0' 'setuptools==69.5.1' 'wheel==0.48.0' 'numpy==1.26.4'
python -m pip install --no-use-pep517 --no-build-isolation -r requirements-ml.txt
python -m pip check
```

LightFM 1.17 có lỗi setup với build backend mặc định (`__builtins__` dạng dict). Cách cài legacy với pip/setuptools trên đã chạy thành công; cần GCC để build extension. Không sửa code thư viện. Python hệ thống ở máy này là 3.14 nên đã tải Python 3.11.17 và giữ runtime trong `.venv/runtime/`, không phụ thuộc thư mục /tmp. Có thể dựng lại bằng uv trên Linux x86_64:

```bash
uv python install 3.11.17 --install-dir .venv/runtime --no-bin
uv venv --allow-existing --python .venv/runtime/cpython-3.11.17-linux-x86_64-gnu/bin/python3.11 .venv
source .venv/bin/activate
# Sau đó chạy các lệnh pip ở trên.
```

### Chỉ khi cần nạp lại database mới qua ETL đã được cho phép

Các lệnh sau có thao tác ghi Warehouse; không cần chạy cho demo thông thường:

```bash
source .venv/bin/activate
set -a
source .env
set +a
RAW_STORAGE=local STAGING_DIR=ml_artifacts/staging python etl_process.py
python recommend.py --train
```

Không chạy ingest_to_minio.py khi dùng raw local. Không cần Airflow, dashboard hay API server để demo phần ML.

## D. Kết quả thực tế

### Chia dữ liệu và features

- Positive: rating >=4, mỗi cặp có interaction=1; không biến tất cả phim chưa rating thành nhãn không thích.
- Holdout theo user, seed=42, khoảng 20% positive mỗi user (lấy floor, tối thiểu 1 test). Có 608 user có >=2 positive để đánh giá; user 214 chỉ có 1 positive, giữ ở train; user 442 không có positive, dùng baseline khi demo. Tổng cộng 609 user có positive trong mapping LightFM, trên 610 user có rating.
- Train: **39.100** positive; test: **9.480**; user đánh giá: **608**. Giao cặp train/test = 0; user test thiếu train = 0.
- LightFM WARP: 32 components, 30 epochs, 1 thread, random_state=42. Không tune trên test.
- Item features: identity + genres MovieLens + tối đa 300 token overview TMDB, binary CountVectorizer, bỏ English stop words và từ xuất hiện dưới 2 document. Vocabulary chỉ fit trên overview thực của các phim có positive train. Token/genres dùng chung giúp model chia sẻ thông tin nội dung.
- Cả hai model xếp hạng toàn bộ catalog 9.742 phim, không negative sampling ở đánh giá. Baseline chỉ đếm số user positive trong train; hòa điểm theo movie_id.
- Offline evaluation loại positive train và rating <4 đã biết, nhưng giữ positive test làm ứng viên. Nếu loại toàn bộ lịch sử tại bước này thì test cũng bị loại và metric sai. Demo thực tế thì loại mọi rating của user, kể cả rating thấp.
- Precision=hit/10; Recall=hit/số positive test của user; NDCG dùng relevance nhị phân, giảm trọng số theo log2(rank+1), IDCG lấy min(10, số positive test). Báo cáo trung bình theo cùng 608 user.

### Metrics từ lần chạy thực tế

| Model | Precision@10 | Recall@10 | NDCG@10 |
|---|---:|---:|---:|
| Most Popular | 0.127961 | 0.108645 | 0.171856 |
| LightFM + WARP | 0.189309 | 0.170421 | 0.247525 |

LightFM cao hơn baseline ở cả ba metric trên split này. Kết quả phù hợp với việc model có thể học sở thích riêng thay vì chỉ dùng độ phổ biến chung; chưa chứng minh riêng phần overview TMDB tạo ra mức tăng vì không thực hiện ablation. Không kết luận mô hình luôn tốt hơn ở mọi split hoặc trong sử dụng thực tế.

### Top 10 thực tế, user_id=15

1. Lord of the Rings: The Return of the King, The (`movie_id=7153`) — 1.918277
2. Jurassic Park (`movie_id=480`) — 1.902063
3. Eternal Sunshine of the Spotless Mind (`movie_id=7361`) — 1.767160
4. V for Vendetta (`movie_id=44191`) — 1.645883
5. Silence of the Lambs, The (`movie_id=593`) — 1.538720
6. Good Will Hunting (`movie_id=1704`) — 1.538101
7. Truman Show, The (`movie_id=1682`) — 1.528276
8. X-Men (`movie_id=3793`) — 1.507964
9. Donnie Darko (`movie_id=4878`) — 1.506416
10. Braveheart (`movie_id=110`) — 1.369311

Điểm là score xếp hạng, không phải xác suất/phần trăm thích phim. Demo dùng model refit trên toàn bộ positive; điểm tuyệt đối không dùng để so sánh giữa các lần train.

### Kiểm chứng sau khi chạy

- Đọc Warehouse và JOIN trực tiếp thành công; schema live và số dòng được lưu trong metrics.json.
- LightFM evaluation và model refit đều huấn luyện thành công.
- Chạy lại --evaluate: metrics giống kết quả lưu.
- Chia lại với cùng seed: train/test giống hệt.
- Huấn luyện lại model evaluation cùng seed/1 thread: user/item embeddings giống hệt.
- Kiểm tra Top 10 cho **610/610 user**: đủ 10 phim, không lặp, **0 phim đã rating xuất hiện lại**.
- User 999999: báo không tồn tại, fallback Most Popular thành công.
- Kiểm tra thủ công bằng ví dụ nhỏ: singleton ở train, không overlap, tie-break, exclusion và công thức metric đều đạt.
- pip check: không có dependency lỗi; git diff --check và biên dịch Python đạt.

## E. Giải thích để sinh viên bảo vệ

1. **Bài toán gì?** Personalized Top-N Recommendation: đề xuất 10 phim user chưa đánh giá dựa trên lịch sử sở thích. Không dự đoán chính xác phim user xem tiếp theo.
2. **Vì sao lấy Warehouse?** Dùng dữ liệu đã làm sạch và tích hợp của nhóm, giữ đúng luồng đa nguồn và tính nhất quán giữa phân tích và ML.
3. **LightFM là gì?** Mô hình factorization có thể kết hợp tương tác và metadata bằng vector ẩn. Phim được biểu diễn bằng tổng vector identity và các feature, có chuẩn hóa feature khi xây ma trận.
4. **WARP làm gì?** Lấy mẫu ứng viên chưa quan sát, tìm cặp vi phạm thứ hạng và cập nhật để phim positive xếp cao hơn, tập trung đầu danh sách. Ứng viên lấy mẫu không phải nhãn khẳng định user ghét phim.
5. **Rating >=4 làm gì?** Quy ước thành positive nhị phân; 4 và 5 đều là 1 trong interaction matrix. Rating thấp không được đưa thành positive.
6. **Học sở thích và nội dung thế nào?** Học vector user, phim và feature sao cho phim user thích nhận điểm cao; thể loại/từ nội dung dùng chung giúp các phim chia sẻ thông tin.
7. **Baseline Most Popular để làm gì?** Kiểm tra cá nhân hóa có tốt hơn gợi ý phim được nhiều user thích trong train hay không, cùng điều kiện đánh giá.
8. **Ba metric nghĩa gì?** Precision đo tỷ lệ hit trong 10 gợi ý; Recall đo tỷ lệ phim positive test tìm lại được; NDCG thưởng hit ở vị trí cao. Đây là đo trên nhãn quan sát, không phải mọi sở thích thật.
9. **Vì sao đề xuất được phim chưa rating?** Tính score từ vector user và vector phim/feature đã học, không cần mọi cặp user–phim đều có trong train. User không có positive dùng fallback phổ biến.
10. **Hạn chế gì?** Dữ liệu thưa, cold-start, thiên lệch phổ biến, ngưỡng positive làm mất mức độ rating; chỉ 300 phim có TMDB metadata. Random holdout không bảo đảm thứ tự thời gian và không chứng minh mức hài lòng thực. Chưa có kiểm định độ ổn định nhiều seed hay ablation; demo dùng snapshot nên cần retrain khi dữ liệu đổi.

Tham khảo thuật toán: [LightFM chính thức](https://making.lyst.com/lightfm/docs/), [WARP loss](https://making.lyst.com/lightfm/docs/examples/warp_loss.html), [setup LightFM 1.17](https://github.com/lyst/lightfm/blob/1.17/setup.py).
