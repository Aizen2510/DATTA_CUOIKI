# Xóa dữ liệu chạy lại từ đầu trên máy hiện tại

Phạm vi: raw local → Python ETL → PostgreSQL → train → đánh giá → demo. Quy trình này không dùng MinIO/Airflow; MinIO chưa khả dụng ở môi trường đã kiểm tra.

**Các lệnh reset bên dưới xóa toàn bộ Warehouse local trong `dw_data/` và model/log/staging làm việc trong `ml_artifacts/`.** Giữ code, `.env`, `.venv` và dữ liệu nguồn `raw_data/`. Các bản staging/export được lưu cùng repository là tài liệu tham chiếu, không được sử dụng làm đầu vào của lần chạy mới.

## 1. Vào đúng thư mục và bật môi trường

```bash
cd /home/dandelion/Projects/DATTA_CUOIKI
source .venv/bin/activate
```

Môi trường Python 3.11 và dependencies đã cài. Nếu cần cài mới, xem `RECOMMENDATION_AUDIT.md`. `.env` phải tồn tại và chứa cấu hình của dự án, không đưa mật khẩu vào Git.

## 2. Dừng container và xóa dữ liệu đã sinh

Chỉ thực hiện khi chủ động muốn xóa kết quả cũ. `docker compose down` riêng lẻ không xóa bind mount `dw_data/`.

```bash
docker compose down

# Xóa nội dung đúng bind mount Warehouse của dự án.
# Dùng container vì file PostgreSQL có thể thuộc user khác trên host.
docker run --rm --user 0 --entrypoint sh \
  -v "$PWD/dw_data:/reset-data" postgres:15-alpine \
  -c 'find /reset-data -mindepth 1 -maxdepth 1 -exec rm -rf {} +'

# Xóa model, log và staging làm việc của lần trước.
rm -rf -- ml_artifacts
mkdir -p ml_artifacts
```

Không dùng `docker system prune`, không xóa volume của dự án khác. Không xóa raw: đây là dữ liệu nguồn cần dùng lại, không phải kết quả model.

## 3. Khởi tạo PostgreSQL mới trống

```bash
docker compose up -d --wait postgres-dw
docker compose ps
```

Lúc này database vừa được khởi tạo, chưa có bảng nghiệp vụ. Chưa thể train ngay.

## 4. Chạy lại ETL từ dữ liệu nguồn

```bash
set -a
source .env
set +a

RAW_STORAGE=local STAGING_DIR=ml_artifacts/staging \
  uv run --active --no-project python etl_process.py \
  > ml_artifacts/etl.log 2>&1

tail -n 20 ml_artifacts/etl.log
```

Chờ ETL kết thúc thành công. ETL đọc CSV/JSON trong `raw_data/`, sinh staging mới, tạo schema, nạp dimension/bridge/fact và kiểm định. Không đọc pickle cũ hay CSV export để nạp.

Với nguồn hiện có, số dòng dự kiến sau kiểm định: 610 user, 9.742 phim, 19 genres, 22.050 bridge, 7.761 time và 100.836 ratings. Nếu ETL lỗi, xem log và xử lý trước khi train.

## 5. Train mới từ Warehouse vừa nạp

```bash
uv run --active --no-project python recommend.py --train
```

Chương trình đọc PostgreSQL, chia train/test, tạo features, huấn luyện model đánh giá, so sánh Most Popular, refit model demo và lưu artifact mới. Không đọc raw trực tiếp để train.

## 6. Đánh giá và demo

```bash
uv run --active --no-project python recommend.py --evaluate
uv run --active --no-project python recommend.py --user-id 15 --top-k 10
```

Đầu ra gồm metrics và Top 10 phim chưa được user đánh giá. Score LightFM là điểm xếp hạng, không phải xác suất. Sau khi có model, hai lệnh này không cần PostgreSQL đang chạy.

Quy trình bắt đầu lại từ dữ liệu nguồn đã thu thập, không tải lại MovieLens hoặc gọi lại TMDB API. Nếu muốn thu thập nguồn mới thì cần xử lý riêng; việc đó có thể làm thay đổi số dòng và kết quả.

## Lần chạy lại đã kiểm chứng ngày 28/10/2026

Đã thực sự xóa dữ liệu PostgreSQL và `ml_artifacts/`, xác nhận database có 0 bảng trước ETL, sau đó nạp lại từ raw local và train mới. Kiểm tra checksum xác nhận raw không thay đổi. Evaluate và demo chạy thành công; kiểm tra cả 610 user không gợi ý lại phim đã rating.

| Model | Precision@10 | Recall@10 | NDCG@10 |
|---|---:|---:|---:|
| Most Popular | 0,127961 | 0,108645 | 0,171856 |
| LightFM + WARP | 0,189309 | 0,170421 | 0,247525 |

Bằng chứng mới nằm trong `ml_artifacts/reset_verification.json`, `etl.log`, `training.log`, `metrics.json` và `verification.json`.
