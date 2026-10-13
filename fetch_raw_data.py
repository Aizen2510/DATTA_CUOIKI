import os
import json
import time
import requests
import pandas as pd

# Api TheMovieDB 
API_KEY = "7818eada14040a0d32a8e292cfb39e7a"

DATA_DIR = "raw_data"
links_path = os.path.join(DATA_DIR, "links.csv")
output_json_path = os.path.join(DATA_DIR, "tmdb_movies_raw.json")

# Test nhanh 1 request trước để xác nhận key
test_url = f"https://api.themoviedb.org/3/movie/862?api_key={API_KEY}&language=en-US"
res_test = requests.get(test_url, timeout=5)

if res_test.status_code != 200:
    print(f"Xác thực thất bại (HTTP {res_test.status_code}):", res_test.text)
    print("\nKhắc phục: Truy cập lại TMDB -> Settings -> API -> Copy lại chính xác giá trị ở ô 'API Key' (v3 auth).")
    exit(1)

print("Xác thực API Key thành công! Bắt đầu crawl dữ liệu...")

# 2. Đọc links.csv lấy 300 phim
df_links = pd.read_csv(links_path)
valid_tmdb = df_links.dropna(subset=["tmdbId"]).copy()
sample_ids = valid_tmdb["tmdbId"].astype(int).head(300).tolist()

tmdb_records = []
session = requests.Session()

for idx, tmdb_id in enumerate(sample_ids, 1):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={API_KEY}&language=en-US"
    try:
        res = session.get(url, timeout=5)
        if res.status_code == 200:
            movie_json = res.json()
            tmdb_records.append({
                "tmdb_id": movie_json.get("id"),
                "title": movie_json.get("title"),
                "popularity": movie_json.get("popularity"),
                "overview": movie_json.get("overview"),
                "release_date": movie_json.get("release_date"),
                "genres": [g["name"] for g in movie_json.get("genres", [])],
                "original_language": movie_json.get("original_language"),
                "vote_average": movie_json.get("vote_average"),
                "vote_count": movie_json.get("vote_count")
            })
        elif res.status_code == 429:
            time.sleep(1.5)
    except Exception as e:
        print(f"Lỗi id {tmdb_id}: {e}")

    if idx % 50 == 0 or idx == len(sample_ids):
        print(f" -> Đã lấy thành công {len(tmdb_records)}/{idx} bản ghi...")

with open(output_json_path, "w", encoding="utf-8") as f:
    json.dump(tmdb_records, f, ensure_ascii=False, indent=2)

print(f"\nHoàn tất! File '{output_json_path}' đã có {len(tmdb_records)} bản ghi JSON thật.")