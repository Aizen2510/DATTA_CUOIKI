import io
import json
import os
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import numpy as np
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine

# CẤU HÌNH KẾT NỐI
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-datalake"
DW_URL = os.getenv("DW_URL", "postgresql://dw_user:dw_password@localhost:5434/dw_movies")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STAGING_DIR = os.getenv("STAGING_DIR", os.path.join(BASE_DIR, "staging"))

os.makedirs(STAGING_DIR, exist_ok=True)

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def get_dw_engine():
    return create_engine(DW_URL)

# ----------------------------------------------------
# 1. EXTRACT (Trích xuất từ MinIO)
# ----------------------------------------------------
def extract_data():
    """Trích xuất dữ liệu từ MinIO Data Lake và lưu tạm vào staging."""
    print("[1/4] Bắt đầu trích xuất dữ liệu từ MinIO Data Lake...")
    client = get_minio_client()

    def _read_csv(object_name):
        obj = client.get_object(BUCKET_NAME, object_name)
        df = pd.read_csv(io.BytesIO(obj.read()))
        obj.close()
        obj.release_conn()
        return df

    def _read_json(object_name):
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

    df_movies_raw.to_pickle(f"{STAGING_DIR}/raw_movies.pkl")
    df_ratings_raw.to_pickle(f"{STAGING_DIR}/raw_ratings.pkl")
    df_links_raw.to_pickle(f"{STAGING_DIR}/raw_links.pkl")
    df_tmdb_raw.to_pickle(f"{STAGING_DIR}/raw_tmdb.pkl")

    print(f" -> Trích xuất thành công: {len(df_movies_raw)} phim, {len(df_ratings_raw)} ratings, {len(df_tmdb_raw)} TMDB records.")

# ----------------------------------------------------
# 2. TRANSFORM (Làm sạch & Chuẩn hóa mô hình hình sao)
# ----------------------------------------------------
def transform_data():
    """Làm sạch dữ liệu, tích hợp MovieLens + TMDB, tạo các bảng Dimension và Fact."""
    print("[2/4] Bắt đầu quá trình Transform & Chuẩn hóa dữ liệu...")

    df_movies_raw = pd.read_pickle(f"{STAGING_DIR}/raw_movies.pkl")
    df_ratings_raw = pd.read_pickle(f"{STAGING_DIR}/raw_ratings.pkl")
    df_links_raw = pd.read_pickle(f"{STAGING_DIR}/raw_links.pkl")
    df_tmdb_raw = pd.read_pickle(f"{STAGING_DIR}/raw_tmdb.pkl")

    # --- 2.1 Làm sạch Ratings & Tạo Dim_User ---
    df_ratings = df_ratings_raw.drop_duplicates(subset=["userId", "movieId"]).dropna(subset=["userId", "movieId", "rating"]).copy()
    df_ratings["rating_time"] = pd.to_datetime(df_ratings["timestamp"], unit="s")
    df_ratings["time_id"] = df_ratings["rating_time"].dt.strftime("%Y%m%d%H").astype(int)

    dim_user = pd.DataFrame({
        "user_id": df_ratings["userId"].unique()
    }).sort_values("user_id").reset_index(drop=True)

    # --- 2.2 Tạo Dim_Time ---
    dim_time = pd.DataFrame({
        "time_id": df_ratings["time_id"],
        "rating_timestamp": df_ratings["rating_time"],
        "hour": df_ratings["rating_time"].dt.hour,
        "day": df_ratings["rating_time"].dt.day,
        "month": df_ratings["rating_time"].dt.month,
        "quarter": df_ratings["rating_time"].dt.quarter,
        "year": df_ratings["rating_time"].dt.year
    }).drop_duplicates(subset=["time_id"]).sort_values("time_id").reset_index(drop=True)

    # --- 2.3 Tạo Dim_Genre ---
    genre_set = set()
    for item in df_movies_raw["genres"].dropna():
        for g in item.split("|"):
            if g != "(no genres listed)":
                genre_set.add(g)

    dim_genre = pd.DataFrame(sorted(list(genre_set)), columns=["genre_name"]).reset_index()
    dim_genre.rename(columns={"index": "genre_id"}, inplace=True)
    dim_genre["genre_id"] += 1

    # --- 2.4 Tích hợp MovieLens + TMDB & Tạo Dim_Movie ---
    df_movies = df_movies_raw.copy()
    df_movies["release_year"] = df_movies["title"].str.extract(r"\((\d{4})\)")[0]
    df_movies["release_year"] = pd.to_numeric(df_movies["release_year"], errors="coerce").fillna(0).astype(int)
    df_movies["release_decade"] = (df_movies["release_year"] // 10) * 10
    df_movies["clean_title"] = df_movies["title"].str.replace(r"\s*\(\d{4}\)", "", regex=True).str.strip()

    df_movies = df_movies.merge(df_links_raw[["movieId", "tmdbId"]], on="movieId", how="left")

    if not df_tmdb_raw.empty and "tmdb_id" in df_tmdb_raw.columns:
        df_tmdb_selected = df_tmdb_raw[["tmdb_id", "popularity", "overview", "vote_average", "vote_count"]].drop_duplicates(subset=["tmdb_id"])
        df_movies = df_movies.merge(df_tmdb_selected, left_on="tmdbId", right_on="tmdb_id", how="left")
    else:
        df_movies["popularity"] = 0.0
        df_movies["overview"] = None
        df_movies["vote_average"] = np.nan
        df_movies["vote_count"] = 0

    df_movies["popularity"] = df_movies["popularity"].fillna(0.0)
    df_movies["overview"] = df_movies["overview"].fillna("No overview available")

    dim_movie = df_movies[[
        "movieId", "clean_title", "genres", "release_year", "release_decade",
        "tmdbId", "popularity", "vote_average", "overview"
    ]].rename(columns={
        "movieId": "movie_id",
        "clean_title": "title",
        "tmdbId": "tmdb_id"
    }).copy()

    # --- 2.5 Tạo Fact_Rating ---
    fact_rating = df_ratings[["userId", "movieId", "time_id", "rating"]].rename(
        columns={"userId": "user_id", "movieId": "movie_id"}
    ).copy()
    fact_rating = fact_rating[fact_rating["movie_id"].isin(dim_movie["movie_id"])]

    # Lưu kết quả chuẩn hóa vào staging
    dim_user.to_pickle(f"{STAGING_DIR}/dim_user.pkl")
    dim_time.to_pickle(f"{STAGING_DIR}/dim_time.pkl")
    dim_genre.to_pickle(f"{STAGING_DIR}/dim_genre.pkl")
    dim_movie.to_pickle(f"{STAGING_DIR}/dim_movie.pkl")
    fact_rating.to_pickle(f"{STAGING_DIR}/fact_rating.pkl")

    print(f" -> Chuẩn hóa xong: dim_user ({len(dim_user)}), dim_time ({len(dim_time)}), dim_genre ({len(dim_genre)}), dim_movie ({len(dim_movie)}), fact_rating ({len(fact_rating)}).")

# ----------------------------------------------------
# 3. LOAD DIMENSIONS (Nạp bảng chiều vào Postgres)
# ----------------------------------------------------
def load_dimension_tables():
    """Nạp các bảng Dimension vào Data Warehouse."""
    print("[3/4] Nạp các bảng Dimension vào PostgreSQL Data Warehouse...")
    engine = get_dw_engine()

    dim_user = pd.read_pickle(f"{STAGING_DIR}/dim_user.pkl")
    dim_time = pd.read_pickle(f"{STAGING_DIR}/dim_time.pkl")
    dim_genre = pd.read_pickle(f"{STAGING_DIR}/dim_genre.pkl")
    dim_movie = pd.read_pickle(f"{STAGING_DIR}/dim_movie.pkl")

    dim_user.to_sql("dim_user", con=engine, if_exists="replace", index=False, chunksize=5000)
    dim_time.to_sql("dim_time", con=engine, if_exists="replace", index=False, chunksize=5000)
    dim_genre.to_sql("dim_genre", con=engine, if_exists="replace", index=False, chunksize=5000)
    dim_movie.to_sql("dim_movie", con=engine, if_exists="replace", index=False, chunksize=5000)
    print(" -> Nạp thành công tất cả Dimension tables.")

# ----------------------------------------------------
# 4. LOAD FACT (Nạp bảng sự kiện vào Postgres)
# ----------------------------------------------------
def load_fact_rating():
    """Nạp bảng Fact Rating vào Data Warehouse."""
    print("[4/4] Nạp bảng fact_rating vào PostgreSQL Data Warehouse...")
    engine = get_dw_engine()

    fact_rating = pd.read_pickle(f"{STAGING_DIR}/fact_rating.pkl")
    fact_rating.to_sql("fact_rating", con=engine, if_exists="replace", index=False, chunksize=10000)
    print(" -> Nạp thành công fact_rating. Hoàn tất quy trình ETL!")

if __name__ == "__main__":
    extract_data()
    transform_data()
    load_dimension_tables()
    load_fact_rating()
