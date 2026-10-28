# MovieLab — chạy pipeline và demo Recommendation trên web

Web local dùng HTML/CSS/JavaScript + Flask, tái sử dụng ETL và LightFM của dự án. Không cần Node, React hoặc Streamlit để chạy. Bố cục, màu sắc và tương tác được sửa trực tiếp trong `templates/index.html`.

## 1. Khởi chạy

Tại thư mục dự án, với `.venv` Python 3.11 và dependencies ML đã chuẩn bị:

```bash
source .venv/bin/activate
uv pip install --python .venv/bin/python -r requirements-web.txt
uv run --active --no-project python web_app.py
```

Mở **http://127.0.0.1:5050**. Giữ terminal chạy web. Nếu cài ML từ máy mới, xem phần dependencies trong `RECOMMENDATION_AUDIT.md`; cần cài cả ML và web. `.env` phải có cấu hình Warehouse hợp lệ và phù hợp volume PostgreSQL hiện có.

Web chỉ lắng nghe localhost, dùng một process, không bật debug/reloader. Không chạy thêm một bản web hoặc train CLI cùng lúc với tác vụ trên web. Đóng tab trình duyệt không dừng tác vụ; giữ tiến trình Python đang chạy đến khi tác vụ kết thúc. Nếu dừng server khi có job, cần kiểm tra các tiến trình con trước khi chạy tiếp.

## 2. Các màn hình

### Tổng quan

- Truy vấn PostgreSQL thật để hiển thị số user, phim và rating hiện tại.
- Số phim có metadata TMDB và metrics lấy từ snapshot model; được ghi nhãn riêng.
- Kiểm tra sự tồn tại của 5 file raw, trạng thái kết nối DB và thời gian cập nhật model.
- Nút Làm mới kiểm tra lại trạng thái, không ghi Warehouse.

DB chưa chạy sẽ hiển thị chưa kết nối; đây không phải lỗi khiến web không mở được. Demo vẫn dùng được nếu đã có model.

### Chạy pipeline

Nút **Chạy toàn bộ pipeline** chạy lần lượt:

1. Đọc và kiểm tra header/số dòng CSV cùng cấu trúc JSON nguồn trong `raw_data/`.
2. `docker compose up -d --wait --wait-timeout 90 postgres-dw`.
3. `etl_process.py` với `RAW_STORAGE=local`, staging mới tại `ml_artifacts/staging/`.
4. `recommend.py --train --epochs ... --seed ...`.
5. `recommend.py --evaluate` để tính lại kết quả trên model đánh giá vừa lưu.

Cần Docker đang hoạt động và quyền truy cập Docker trên máy chạy web. ETL và train dùng chính Python của môi trường đang chạy Flask (`sys.executable`). Các bước chạy dưới tiến trình riêng, không đóng băng trình duyệt; giao diện hỏi trạng thái/log mỗi 1,5 giây. Bước lỗi sẽ dừng các bước sau và giữ exit code/log. Không có phần trăm huấn luyện giả lập.

Có thể bấm riêng kiểm tra raw, khởi động DB, ETL, train hoặc đánh giá. ETL riêng không tự khởi động DB: nếu DB chưa bật, hãy bấm Khởi động DB trước. Một server chỉ nhận một job tại một thời điểm; demo bị khóa trong lúc job chạy để tránh đọc model đang cập nhật.

Luồng dùng raw đã thu thập, không tải MovieLens hay gọi thêm TMDB API. Không điều phối qua Airflow hoặc MinIO. ETL thực hiện upsert theo code hiện có, không reset dữ liệu. Muốn xóa hoàn toàn và làm lại, thực hiện có chủ đích theo `RUN_FROM_SCRATCH.md`; không có nút xóa dữ liệu trên web.

### Đánh giá model

Hiển thị metrics của Most Popular và LightFM + WARP lấy từ `bundle.report` trong artifact, cùng số user đánh giá, train/test, seed và epochs. Biểu đồ dùng cùng các số thực tế; thanh có thang 0–1. Nút Đánh giá lại chạy tính toán thật và đưa người dùng sang nhật ký để xem kết quả mới.

`evaluation_model` học positive train để báo metric test. Model demo được refit trên toàn bộ positive. Metadata/vocabulary và cách chia tập giữ nguyên như CLI. Việc bấm demo không làm thay đổi metrics.

### Gợi ý phim

- Nhập user ID và chọn Top 5/10/20.
- Gọi trực tiếp hàm `recommend()` trong `recommend.py`, không sao chép thuật toán sang frontend.
- Hiển thị tên phim, thể loại, ID và score; bên cạnh là lịch sử rating từ cùng snapshot.
- User lạ hoặc không có positive dùng Most Popular và được ghi rõ fallback.
- Backend kiểm tra tập kết quả không giao lịch sử trước khi trả dữ liệu.
- Score LightFM là điểm xếp hạng; fallback dùng số lượt user positive, không phải xác suất.

Demo không truy vấn live Warehouse. Khi DB có ratings mới, cần train lại để cập nhật model/lịch sử. Sau train thành công, server tự tải lại artifact mới, không giữ model cũ trong cache.

## 3. File và trách nhiệm

| File | Vai trò |
|---|---|
| `web_app.py` | Routes JSON, kiểm tra nguồn/DB, worker chạy lệnh cố định, khóa job, đọc model và trả gợi ý |
| `templates/index.html` | Giao diện responsive, CSS, điều hướng, fetch API, polling log và render kết quả |
| `requirements-web.txt` | Flask và dependency được giải quyết khi cài |
| `tests/test_web.py` | Kiểm tra lỗi, giới hạn đầu vào, job chồng, thiếu model, dừng khi bước lỗi và ẩn secret |
| `recommend.py` | Giữ thuật toán cũ; lưu model/metrics qua file tạm rồi replace để tránh ghi trực tiếp đè model đang dùng |

Các thay đổi README và `RUN_FROM_SCRATCH.md` có từ trước được giữ lại. Không sửa schema, thuật toán ETL hoặc dashboard Power BI để làm web này.

## 4. API local

| API | Tác dụng |
|---|---|
| `GET /api/status` | Raw file status, số dòng Warehouse live, báo cáo snapshot model |
| `POST /api/jobs` | Bắt đầu một action trong allowlist: check/database/etl/train/evaluate/pipeline |
| `GET /api/job` | Trạng thái từng bước và phần cuối log |
| `GET /api/recommendations?user_id=15&top_k=10` | Kết quả model và lịch sử user |

POST dùng JSON và header `X-Demo-Request: 1` do giao diện gửi. Không có API nhận shell command, upload pickle, đọc file tùy ý hoặc hiển thị `.env`. Log trả về trình duyệt được lọc các secret cấu hình. Đây là công cụ local phục vụ bài tập, chưa thiết kế thành dịch vụ public nhiều người dùng.

## 5. Kiểm chứng thực tế

Ngày 28/10/2026:

- Đã chạy toàn bộ pipeline qua HTTP từ trạng thái PostgreSQL tắt: khởi động DB, ETL/validate, train 30 epochs seed 42 và evaluate đều thành công.
- Warehouse sau ETL có 610 user, 9.742 phim và 100.836 ratings.
- API demo user 15 trả 10 phim, lịch sử 135 phim, không giao nhau.
- User 442 (không có positive) và 999999 (không tồn tại) dùng fallback thành công, không trùng lịch sử.
- Kiểm thử trình duyệt Chrome thật: chuyển tab, hiển thị metrics, gửi form user 15, fallback user 442, nút kiểm tra raw và polling trạng thái.
- Kiểm tra viewport desktop 1.440 px và mobile 390 px: không tràn ngang, không lỗi JavaScript trong các luồng đã thử.

| Model | Precision@10 | Recall@10 | NDCG@10 |
|---|---:|---:|---:|
| Most Popular | 0,127961 | 0,108645 | 0,171856 |
| LightFM + WARP | 0,189309 | 0,170421 | 0,247525 |

Bằng chứng ở `ml_artifacts/web_integration_verification.json`, `web_browser_verification.json`, `web_*.log`, `web_last_job.json`. Ảnh chụp giao diện: `web-overview.png`, `web-demo.png`, `web-mobile.png`. Các file này là artifact local bị Git bỏ qua.

Chạy kiểm thử backend:

```bash
uv run --active --no-project python -m unittest discover -s tests -v
```

Tham khảo: [Flask Quickstart](https://flask.palletsprojects.com/en/stable/quickstart/), [Flask testing](https://flask.palletsprojects.com/en/stable/testing/).
