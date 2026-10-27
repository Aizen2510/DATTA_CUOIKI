"""
Module ETL MovieLens + TMDB vào Data Warehouse PostgreSQL theo mô hình Star Schema chuẩn.
Bao gồm:
- Trích xuất từ MinIO Data Lake
- Làm sạch, khử trùng lặp, chuẩn hóa mô hình hình sao (Star Schema) có Bridge Table
- Khởi tạo DDL chuẩn có đầy đủ ràng buộc Primary Key, Foreign Key và Indexes
- Nạp an toàn qua transaction và append
- Kiểm định chất lượng dữ liệu tự động (Data Quality Check)
"""

import io
import json
import logging
import os
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import numpy as np
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, inspect, text

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ETL_Process")

# ============================================================
# CẤU HÌNH KẾT NỐI
# ============================================================
# CẤU HÌNH KẾT NỐI
# ============================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "raw-datalake")

DW_URL = os.getenv("DW_URL")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STAGING_DIR = os.getenv("STAGING_DIR", os.path.join(BASE_DIR, "staging"))
os.makedirs(STAGING_DIR, exist_ok=True)


def get_minio_client():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in the environment.")
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def get_dw_engine():
    if not DW_URL:
        raise RuntimeError("Set DW_URL in the environment; credentials must not be hard-coded.")
    return create_engine(DW_URL)


# ============================================================
# KHỞI TẠO SCHEMA DDL VỚI PRIMARY KEY, FOREIGN KEY, INDEXES
# ============================================================
def init_schema():
    """Khởi tạo cấu trúc các bảng Star Schema với ràng buộc toàn vẹn PK/FK và Index."""
    logger.info("Kiểm tra và khởi tạo DDL schema Data Warehouse (Star Schema + Bridge)...")
    engine = get_dw_engine()

    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_user (
            user_id INT PRIMARY KEY
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_movie (
            movie_id INT PRIMARY KEY,
            title TEXT NOT NULL,
            genres TEXT,
            release_year INT,
            release_decade INT,
            tmdb_id INT,
            popularity FLOAT,
            vote_average FLOAT,
            vote_count INT,
            overview TEXT,
            has_tmdb_metadata BOOLEAN DEFAULT FALSE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_genre (
            genre_id INT PRIMARY KEY,
            genre_name VARCHAR(100) UNIQUE NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS bridge_movie_genre (
            movie_id INT NOT NULL REFERENCES dim_movie(movie_id) ON DELETE CASCADE,
            genre_id INT NOT NULL REFERENCES dim_genre(genre_id) ON DELETE CASCADE,
            PRIMARY KEY (movie_id, genre_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id BIGINT PRIMARY KEY,
            hour_timestamp TIMESTAMP NOT NULL,
            hour INT NOT NULL,
            day INT NOT NULL,
            month INT NOT NULL,
            quarter INT NOT NULL,
            year INT NOT NULL,
            day_of_week INT NOT NULL,
            day_name VARCHAR(20) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_rating (
            user_id INT NOT NULL REFERENCES dim_user(user_id) ON DELETE CASCADE,
            movie_id INT NOT NULL REFERENCES dim_movie(movie_id) ON DELETE CASCADE,
            time_id BIGINT NOT NULL REFERENCES dim_time(time_id) ON DELETE CASCADE,
            rating FLOAT NOT NULL,
            rating_timestamp TIMESTAMP NOT NULL,
            PRIMARY KEY (user_id, movie_id)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_fact_user ON fact_rating(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_fact_movie ON fact_rating(movie_id);",
        "CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_rating(time_id);",
        "CREATE INDEX IF NOT EXISTS idx_bridge_movie ON bridge_movie_genre(movie_id);",
        "CREATE INDEX IF NOT EXISTS idx_bridge_genre ON bridge_movie_genre(genre_id);"
    ]

    with engine.begin() as conn:
        for stmt in ddl_statements:
            conn.execute(text(stmt))

    logger.info("Khởi tạo cấu trúc schema và các ràng buộc PK/FK thành công.")


# ============================================================
# 1. EXTRACT (Trích xuất từ MinIO Data Lake)
# ============================================================
def extract_data():
    """Đọc raw từ MinIO (mặc định) hoặc thư mục local được chọn rõ ràng."""
    raw_storage = os.getenv("RAW_STORAGE", "minio")
    if raw_storage not in {"minio", "local"}:
        raise ValueError("RAW_STORAGE must be minio or local")
    data_dir = os.getenv("DATA_DIR", os.path.join(BASE_DIR, "raw_data"))
    logger.info("[1/4] Trích xuất raw từ %s", raw_storage)
    client = get_minio_client() if raw_storage == "minio" else None

    def _read_csv(object_name):
        if raw_storage == "local":
            return pd.read_csv(os.path.join(data_dir, os.path.basename(object_name)))
        obj = client.get_object(BUCKET_NAME, object_name)
        df = pd.read_csv(io.BytesIO(obj.read()))
        obj.close()
        obj.release_conn()
        return df

    def _read_json(object_name):
        if raw_storage == "local":
            with open(os.path.join(data_dir, os.path.basename(object_name)), encoding="utf-8") as source:
                return json.load(source)
        obj = client.get_object(BUCKET_NAME, object_name)
        data = json.loads(obj.read().decode("utf-8"))
        obj.close()
        obj.release_conn()
        return data

    df_movies_raw = _read_csv("movielens/movies.csv")
    df_ratings_raw = _read_csv("movielens/ratings.csv")
    df_links_raw = _read_csv("movielens/links.csv")
    tmdb_raw_list = _read_json("tmdb/tmdb_movies_raw.json")
    df_tmdb_raw = pd.DataFrame(tmdb_raw_list)

    df_movies_raw.to_pickle(os.path.join(STAGING_DIR, "raw_movies.pkl"))
    df_ratings_raw.to_pickle(os.path.join(STAGING_DIR, "raw_ratings.pkl"))
    df_links_raw.to_pickle(os.path.join(STAGING_DIR, "raw_links.pkl"))
    df_tmdb_raw.to_pickle(os.path.join(STAGING_DIR, "raw_tmdb.pkl"))

    logger.info(
        f"Trích xuất thành công: {len(df_movies_raw)} phim, "
        f"{len(df_ratings_raw)} ratings, {len(df_tmdb_raw)} TMDB records."
    )


# ============================================================
# 2. TRANSFORM (Làm sạch & Chuẩn hóa mô hình hình sao)
# ============================================================
def transform_data():
    """Làm sạch dữ liệu, tích hợp MovieLens + TMDB, tạo các bảng Dimension, Bridge và Fact."""
    logger.info("[2/4] Bắt đầu quá trình Transform & Chuẩn hóa dữ liệu...")

    df_movies_raw = pd.read_pickle(os.path.join(STAGING_DIR, "raw_movies.pkl"))
    df_ratings_raw = pd.read_pickle(os.path.join(STAGING_DIR, "raw_ratings.pkl"))
    df_links_raw = pd.read_pickle(os.path.join(STAGING_DIR, "raw_links.pkl"))
    df_tmdb_raw = pd.read_pickle(os.path.join(STAGING_DIR, "raw_tmdb.pkl"))

    # --- 2.1 Làm sạch Ratings & Tạo Dim_User ---
    df_ratings = df_ratings_raw.dropna(subset=["userId", "movieId", "rating"]).copy()
    # Khử trùng lặp trên cặp (userId, movieId), giữ bản ghi mới nhất theo timestamp
    df_ratings = df_ratings.sort_values("timestamp").drop_duplicates(subset=["userId", "movieId"], keep="last")
    df_ratings["rating_time"] = pd.to_datetime(df_ratings["timestamp"], unit="s")
    df_ratings["time_id"] = df_ratings["rating_time"].dt.strftime("%Y%m%d%H").astype(np.int64)

    dim_user = pd.DataFrame({
        "user_id": df_ratings["userId"].astype(int).unique()
    }).sort_values("user_id").reset_index(drop=True)

    # --- 2.2 Tạo Dim_Time (Độ chi tiết nhất quán grain theo giờ) ---
    hour_timestamp = df_ratings["rating_time"].dt.floor("h")
    dim_time = pd.DataFrame({
        "time_id": df_ratings["time_id"],
        "hour_timestamp": hour_timestamp,
        "hour": hour_timestamp.dt.hour.astype(int),
        "day": hour_timestamp.dt.day.astype(int),
        "month": hour_timestamp.dt.month.astype(int),
        "quarter": hour_timestamp.dt.quarter.astype(int),
        "year": hour_timestamp.dt.year.astype(int),
        "day_of_week": hour_timestamp.dt.dayofweek.astype(int),
        "day_name": hour_timestamp.dt.day_name().astype(str)
    }).drop_duplicates(subset=["time_id"]).sort_values("time_id").reset_index(drop=True)

    # --- 2.3 Tạo Dim_Genre & Bridge_Movie_Genre (Quan hệ nhiều-nhiều Phim - Thể loại) ---
    genre_set = set()
    for item in df_movies_raw["genres"].dropna():
        for g in item.split("|"):
            g = g.strip()
            if g and g != "(no genres listed)":
                genre_set.add(g)

    dim_genre = pd.DataFrame(sorted(list(genre_set)), columns=["genre_name"]).reset_index()
    dim_genre.rename(columns={"index": "genre_id"}, inplace=True)
    dim_genre["genre_id"] = dim_genre["genre_id"].astype(int) + 1
    genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))

    bridge_rows = []
    for _, row in df_movies_raw.iterrows():
        m_id = int(row["movieId"])
        genres_str = str(row["genres"]) if pd.notna(row["genres"]) else ""
        for g in genres_str.split("|"):
            g = g.strip()
            if g in genre_map:
                bridge_rows.append({"movie_id": m_id, "genre_id": int(genre_map[g])})

    bridge_movie_genre = pd.DataFrame(bridge_rows).drop_duplicates().sort_values(["movie_id", "genre_id"]).reset_index(drop=True)

    # --- 2.4 Tích hợp MovieLens + TMDB & Tạo Dim_Movie ---
    df_movies = df_movies_raw.copy()
    df_movies["release_year"] = df_movies["title"].str.extract(r"\((\d{4})\)")[0]
    df_movies["release_year"] = pd.to_numeric(df_movies["release_year"], errors="coerce").fillna(0).astype(int)
    df_movies["release_decade"] = (df_movies["release_year"] // 10) * 10
    df_movies["clean_title"] = df_movies["title"].str.replace(r"\s*\(\d{4}\)", "", regex=True).str.strip()

    df_links_clean = df_links_raw.dropna(subset=["movieId"]).copy()
    df_links_clean["movieId"] = df_links_clean["movieId"].astype(int)
    df_movies = df_movies.merge(df_links_clean[["movieId", "tmdbId"]], on="movieId", how="left")

    if not df_tmdb_raw.empty and "tmdb_id" in df_tmdb_raw.columns:
        df_tmdb_selected = df_tmdb_raw[["tmdb_id", "popularity", "overview", "vote_average", "vote_count"]].drop_duplicates(subset=["tmdb_id"]).copy()
        df_tmdb_selected["tmdb_id"] = pd.to_numeric(df_tmdb_selected["tmdb_id"], errors="coerce")
        df_movies = df_movies.merge(df_tmdb_selected, left_on="tmdbId", right_on="tmdb_id", how="left")
    else:
        df_movies["popularity"] = 0.0
        df_movies["overview"] = None
        df_movies["vote_average"] = None
        df_movies["vote_count"] = 0
        df_movies["tmdb_id"] = None

    # Phân định rõ ràng: có metadata TMDB thật vs fallback mặc định
    df_movies["has_tmdb_metadata"] = df_movies["tmdb_id"].notna()
    df_movies["popularity"] = pd.to_numeric(df_movies["popularity"], errors="coerce").fillna(0.0)
    df_movies["vote_average"] = pd.to_numeric(df_movies["vote_average"], errors="coerce")  # giữ NaN/Null cho phim không có TMDB
    df_movies["vote_count"] = pd.to_numeric(df_movies["vote_count"], errors="coerce").fillna(0).astype(int)
    df_movies["overview"] = df_movies["overview"].fillna("No overview available")

    # Xử lý tmdb_id là Int64 nullable
    df_movies["tmdb_id"] = df_movies["tmdbId"].astype("Int64")

    dim_movie = df_movies[[
        "movieId", "clean_title", "genres", "release_year", "release_decade",
        "tmdb_id", "popularity", "vote_average", "vote_count", "overview", "has_tmdb_metadata"
    ]].rename(columns={
        "movieId": "movie_id",
        "clean_title": "title"
    }).copy()
    dim_movie["movie_id"] = dim_movie["movie_id"].astype(int)

    # --- 2.5 Tạo Fact_Rating ---
    fact_rating = df_ratings[["userId", "movieId", "time_id", "rating", "rating_time"]].rename(
        columns={
            "userId": "user_id",
            "movieId": "movie_id",
            "rating_time": "rating_timestamp"
        }
    ).copy()

    fact_rating["user_id"] = fact_rating["user_id"].astype(int)
    fact_rating["movie_id"] = fact_rating["movie_id"].astype(int)
    fact_rating["time_id"] = fact_rating["time_id"].astype(np.int64)
    fact_rating["rating"] = fact_rating["rating"].astype(float)

    # Ràng buộc toàn vẹn: chỉ giữ rating có movie và user hợp lệ trong Dimension
    fact_rating = fact_rating[
        fact_rating["movie_id"].isin(dim_movie["movie_id"]) &
        fact_rating["user_id"].isin(dim_user["user_id"]) &
        fact_rating["time_id"].isin(dim_time["time_id"])
    ].reset_index(drop=True)

    # Lưu kết quả chuẩn hóa vào staging
    dim_user.to_pickle(os.path.join(STAGING_DIR, "dim_user.pkl"))
    dim_time.to_pickle(os.path.join(STAGING_DIR, "dim_time.pkl"))
    dim_genre.to_pickle(os.path.join(STAGING_DIR, "dim_genre.pkl"))
    bridge_movie_genre.to_pickle(os.path.join(STAGING_DIR, "bridge_movie_genre.pkl"))
    dim_movie.to_pickle(os.path.join(STAGING_DIR, "dim_movie.pkl"))
    fact_rating.to_pickle(os.path.join(STAGING_DIR, "fact_rating.pkl"))

    logger.info(
        f"Transform hoàn tất: dim_user ({len(dim_user)}), dim_time ({len(dim_time)}), "
        f"dim_genre ({len(dim_genre)}), bridge_movie_genre ({len(bridge_movie_genre)}), "
        f"dim_movie ({len(dim_movie)}), fact_rating ({len(fact_rating)})."
    )


# ============================================================
# 3. LOAD DIMENSIONS & BRIDGE (Nạp bảng chiều vào Postgres)
# ============================================================
def _upsert_dataframe(conn, dataframe, table_name, key_columns, update_columns):
    """Load a dataframe through a transaction-scoped staging table and PostgreSQL upsert."""
    if dataframe.empty:
        logger.warning("Không có dòng mới để nạp vào %s.", table_name)
        return
    staging_table = f"_etl_stage_{table_name}"
    dataframe.to_sql(staging_table, con=conn, if_exists="replace", index=False, chunksize=10000)
    quoted_columns = ", ".join(f'"{column}"' for column in dataframe.columns)
    conflict_keys = ", ".join(f'"{column}"' for column in key_columns)
    if update_columns:
        update_clause = ", ".join(
            f'"{column}" = EXCLUDED."{column}"' for column in update_columns
        )
        conflict_action = f"DO UPDATE SET {update_clause}"
    else:
        conflict_action = "DO NOTHING"
    conn.execute(text(
        f'INSERT INTO "{table_name}" ({quoted_columns}) '
        f'SELECT {quoted_columns} FROM "{staging_table}" '
        f'ON CONFLICT ({conflict_keys}) {conflict_action};'
    ))
    conn.execute(text(f'DROP TABLE "{staging_table}";'))


def _verify_conflict_keys(engine, table_name, key_columns):
    """Fail before loading if an existing table lacks the unique key required for upsert."""
    inspector = inspect(engine)
    primary_key = set(inspector.get_pk_constraint(table_name).get("constrained_columns") or [])
    unique_keys = [
        set(item.get("column_names") or [])
        for item in inspector.get_unique_constraints(table_name)
    ]
    required = set(key_columns)
    if required != primary_key and required not in unique_keys:
        raise RuntimeError(
            f"Table {table_name} needs a PRIMARY KEY or UNIQUE constraint on "
            f"({', '.join(key_columns)}) for safe upsert. Existing tables are not altered "
            "automatically; inspect and back up the database before applying a schema migration."
        )


def load_dimension_tables():
    """Upsert dimensions and bridge atomically; existing DW rows are retained."""
    logger.info("[3/4] Nạp/upsert các bảng Dimension & Bridge...")
    init_schema()
    engine = get_dw_engine()
    dataframes = {
        "dim_user": pd.read_pickle(os.path.join(STAGING_DIR, "dim_user.pkl")),
        "dim_movie": pd.read_pickle(os.path.join(STAGING_DIR, "dim_movie.pkl")),
        "dim_genre": pd.read_pickle(os.path.join(STAGING_DIR, "dim_genre.pkl")),
        "bridge_movie_genre": pd.read_pickle(os.path.join(STAGING_DIR, "bridge_movie_genre.pkl")),
        "dim_time": pd.read_pickle(os.path.join(STAGING_DIR, "dim_time.pkl")),
    }
    upsert_spec = [
        ("dim_user", ["user_id"], []),
        ("dim_movie", ["movie_id"], ["title", "genres", "release_year", "release_decade", "tmdb_id",
                                     "popularity", "vote_average", "vote_count", "overview", "has_tmdb_metadata"]),
        ("dim_genre", ["genre_id"], ["genre_name"]),
        ("dim_time", ["time_id"], ["hour_timestamp", "hour", "day", "month", "quarter", "year",
                                   "day_of_week", "day_name"]),
        ("bridge_movie_genre", ["movie_id", "genre_id"], []),
    ]
    for table, keys, _updates in upsert_spec:
        _verify_conflict_keys(engine, table, keys)
    with engine.begin() as conn:
        for table, keys, updates in upsert_spec:
            _upsert_dataframe(conn, dataframes[table], table, keys, updates)
            logger.info(" -> Đã upsert %s: %s dòng đầu vào.", table, len(dataframes[table]))
    logger.info("Hoàn tất upsert dimensions và bridge. Không xóa dữ liệu cũ.")


# ============================================================
# 4. LOAD FACT (Nạp bảng sự kiện vào Postgres)
# ============================================================
def load_fact_rating():
    """Upsert fact ratings by the existing (user_id, movie_id) natural key."""
    logger.info("[4/4] Upsert bảng fact_rating vào PostgreSQL Data Warehouse...")
    engine = get_dw_engine()
    fact_rating = pd.read_pickle(os.path.join(STAGING_DIR, "fact_rating.pkl"))
    _verify_conflict_keys(engine, "fact_rating", ["user_id", "movie_id"])
    with engine.begin() as conn:
        _upsert_dataframe(
            conn, fact_rating, "fact_rating", ["user_id", "movie_id"],
            ["time_id", "rating", "rating_timestamp"],
        )
    logger.info(" -> Đã upsert fact_rating: %s dòng đầu vào.", len(fact_rating))


# ============================================================
# 5. DATA QUALITY CHECK (Kiểm định chất lượng dữ liệu tự động)
# ============================================================
def validate_data_warehouse():
    """
    Kiểm tra tự động toàn diện chất lượng dữ liệu:
    1. Số lượng bản ghi > 0 cho tất cả 6 bảng.
    2. Tính duy nhất và không Null trên các Primary Key.
    3. Toàn vẹn tham chiếu (Referential Integrity): không có orphan records.
    4. Miền giá trị Rating [0.5, 5.0].
    5. Thống kê tỷ lệ metadata TMDB.
    """
    logger.info("[Data Quality] Bắt đầu kiểm định chất lượng dữ liệu tự động...")
    engine = get_dw_engine()

    checks_passed = True
    errors = []

    with engine.connect() as conn:
        # 1. Kiểm tra Row counts
        tables = ["dim_user", "dim_movie", "dim_genre", "bridge_movie_genre", "dim_time", "fact_rating"]
        for tbl in tables:
            cnt = conn.execute(text(f"SELECT count(*) FROM {tbl};")).scalar()
            logger.info(f" [Check 1] Bảng '{tbl}' có {cnt} bản ghi.")
            if cnt == 0:
                checks_passed = False
                errors.append(f"Bảng {tbl} trống (0 bản ghi).")

        # 2. Kiểm tra Null & Duplicates trên Primary Key
        pk_checks = [
            ("dim_user", "user_id"),
            ("dim_movie", "movie_id"),
            ("dim_genre", "genre_id"),
            ("dim_time", "time_id"),
        ]
        for tbl, pk_col in pk_checks:
            null_cnt = conn.execute(text(f"SELECT count(*) FROM {tbl} WHERE {pk_col} IS NULL;")).scalar()
            dup_cnt = conn.execute(text(f"SELECT count(*) FROM (SELECT {pk_col} FROM {tbl} GROUP BY {pk_col} HAVING count(*) > 1) t;")).scalar()
            if null_cnt > 0 or dup_cnt > 0:
                checks_passed = False
                errors.append(f"Bảng {tbl} vi phạm PK ({pk_col}): nulls={null_cnt}, dups={dup_cnt}.")

        # Kiểm tra PK phức hợp của bridge_movie_genre và fact_rating
        dup_bridge = conn.execute(text(
            "SELECT count(*) FROM (SELECT movie_id, genre_id FROM bridge_movie_genre GROUP BY movie_id, genre_id HAVING count(*) > 1) t;"
        )).scalar()
        if dup_bridge > 0:
            checks_passed = False
            errors.append(f"bridge_movie_genre có {dup_bridge} cặp (movie_id, genre_id) trùng lặp.")

        dup_fact = conn.execute(text(
            "SELECT count(*) FROM (SELECT user_id, movie_id FROM fact_rating GROUP BY user_id, movie_id HAVING count(*) > 1) t;"
        )).scalar()
        if dup_fact > 0:
            checks_passed = False
            errors.append(f"fact_rating có {dup_fact} cặp (user_id, movie_id) trùng lặp.")

        # 3. Kiểm tra toàn vẹn tham chiếu (Referential Integrity - 0 orphan records)
        orphan_users = conn.execute(text(
            "SELECT count(*) FROM fact_rating f LEFT JOIN dim_user u ON f.user_id = u.user_id WHERE u.user_id IS NULL;"
        )).scalar()
        orphan_movies = conn.execute(text(
            "SELECT count(*) FROM fact_rating f LEFT JOIN dim_movie m ON f.movie_id = m.movie_id WHERE m.movie_id IS NULL;"
        )).scalar()
        orphan_times = conn.execute(text(
            "SELECT count(*) FROM fact_rating f LEFT JOIN dim_time t ON f.time_id = t.time_id WHERE t.time_id IS NULL;"
        )).scalar()
        orphan_bridge_movies = conn.execute(text(
            "SELECT count(*) FROM bridge_movie_genre b LEFT JOIN dim_movie m ON b.movie_id = m.movie_id WHERE m.movie_id IS NULL;"
        )).scalar()
        orphan_bridge_genres = conn.execute(text(
            "SELECT count(*) FROM bridge_movie_genre b LEFT JOIN dim_genre g ON b.genre_id = g.genre_id WHERE g.genre_id IS NULL;"
        )).scalar()

        if orphan_users > 0 or orphan_movies > 0 or orphan_times > 0 or orphan_bridge_movies > 0 or orphan_bridge_genres > 0:
            checks_passed = False
            errors.append(
                f"Vi phạm toàn vẹn tham chiếu FK! "
                f"orphan_users={orphan_users}, orphan_movies={orphan_movies}, orphan_times={orphan_times}, "
                f"orphan_bridge_movies={orphan_bridge_movies}, orphan_bridge_genres={orphan_bridge_genres}."
            )

        # 4. Kiểm tra miền giá trị Rating [0.5, 5.0]
        invalid_ratings = conn.execute(text(
            "SELECT count(*) FROM fact_rating WHERE rating < 0.5 OR rating > 5.0;"
        )).scalar()
        if invalid_ratings > 0:
            checks_passed = False
            errors.append(f"fact_rating có {invalid_ratings} bản ghi rating ngoài dải [0.5, 5.0].")

        # 5. Thống kê metadata TMDB
        tmdb_real_cnt = conn.execute(text(
            "SELECT count(*) FROM dim_movie WHERE has_tmdb_metadata = TRUE;"
        )).scalar()
        total_movies = conn.execute(text("SELECT count(*) FROM dim_movie;")).scalar()
        logger.info(f" [Check 5] Tỷ lệ metadata TMDB: {tmdb_real_cnt}/{total_movies} phim ({tmdb_real_cnt/total_movies*100:.2f}%).")

    if not checks_passed:
        error_msg = "Kiểm tra Data Quality THẤT BẠI:\n - " + "\n - ".join(errors)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("🎉 TẤT CẢ CÁC BƯỚC KIỂM TRA CHẤT LƯỢNG DỮ LIỆU ĐÃ ĐẠT 100%! Data Warehouse sẵn sàng cho BI & Khai phá dữ liệu.")


if __name__ == "__main__":
    extract_data()
    transform_data()
    load_dimension_tables()
    load_fact_rating()
    validate_data_warehouse()
