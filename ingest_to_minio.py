import os
import mimetypes
from minio import Minio
from minio.error import S3Error

#  Cấu hình kết nối MinIO
MINIO_ENDPOINT  = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME      = "raw-datalake"
DATA_DIR         = "raw_data"

# Ánh xạ: tên file local → đường dẫn object trên MinIO (Data Lake)
UPLOAD_MAP = {
    "movies.csv"          : "movielens/movies.csv",
    "ratings.csv"         : "movielens/ratings.csv",
    "links.csv"           : "movielens/links.csv",
    "tags.csv"            : "movielens/tags.csv",
    "tmdb_movies_raw.json": "tmdb/tmdb_movies_raw.json",
}

def upload_raw_to_minio():
    """
    Nạp (ingest) toàn bộ dữ liệu thô từ local vào bucket raw-datalake trên MinIO.
    - Tự động tạo bucket nếu chưa tồn tại.
    - Phân vùng dữ liệu: /movielens (CSV) và /tmdb (JSON).
    - Không xử lý hoặc biến đổi dữ liệu gốc (nguyên tắc Data Lake).
    """

    # 1. Khởi tạo MinIO Client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False          # HTTP nội bộ (không dùng TLS)
    )

    # 2. Tạo bucket raw-datalake nếu chưa tồn tại
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"[+] Đã tạo bucket mới  : '{BUCKET_NAME}'")
        else:
            print(f"[*] Bucket đã tồn tại  : '{BUCKET_NAME}'")
    except S3Error as exc:
        print(f"[ERROR] Không thể kiểm tra/tạo bucket: {exc}")
        raise

    # 3. Ingestion: đẩy từng file từ local lên MinIO
    print("\n" + "="*60)
    print("  BẮT ĐẦU INGESTION DỮ LIỆU THÔ → MinIO Data Lake")
    print("="*60)

    success_count = 0
    for local_file, s3_object_path in UPLOAD_MAP.items():
        local_full_path = os.path.join(DATA_DIR, local_file)

        if not os.path.exists(local_full_path):
            print(f"  [!] BỎ QUA : '{local_file}' không tìm thấy tại '{DATA_DIR}/'")
            continue

        file_size_mb   = os.path.getsize(local_full_path) / (1024 * 1024)
        content_type, _ = mimetypes.guess_type(local_full_path)
        content_type   = content_type or "application/octet-stream"

        try:
            client.fput_object(
                bucket_name  = BUCKET_NAME,
                object_name  = s3_object_path,
                file_path    = local_full_path,
                content_type = content_type
            )
            print(
                f"  [✔] ĐÃ TẢI: {local_file:<30}"
                f"({file_size_mb:6.2f} MB)  →  s3://{BUCKET_NAME}/{s3_object_path}"
            )
            success_count += 1
        except S3Error as exc:
            print(f"  [✘] LỖI khi tải '{local_file}': {exc}")

    print("="*60)
    print(f"  KẾT QUẢ: {success_count}/{len(UPLOAD_MAP)} file đã được nạp thành công.")
    print("="*60 + "\n")


if __name__ == "__main__":
    upload_raw_to_minio()
