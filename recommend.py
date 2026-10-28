"""LightFM + WARP và Most Popular, chỉ huấn luyện từ PostgreSQL Warehouse."""
import argparse
import hashlib
import json
import os
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from lightfm import LightFM
from lightfm.data import Dataset
from scipy.sparse import coo_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "ml_artifacts"


def read_warehouse():
    """Cùng một snapshot, không ghi dữ liệu hay tạo bảng trong Warehouse."""
    load_dotenv(ROOT / ".env")
    url = os.getenv("DW_URL")
    if not url:
        raise ValueError("Thiếu DW_URL: cấu hình biến môi trường hoặc .env.")
    engine = create_engine(url, isolation_level="REPEATABLE READ",
                           connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            schema = pd.read_sql_query(text("""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns WHERE table_schema='public'
                ORDER BY table_name, ordinal_position
            """), conn)
            required = {
                "fact_rating": {"user_id", "movie_id", "rating", "time_id"},
                "dim_movie": {"movie_id", "title", "genres", "overview", "has_tmdb_metadata"},
                "dim_user": {"user_id"}, "dim_time": {"time_id"},
                "dim_genre": {"genre_id", "genre_name"},
                "bridge_movie_genre": {"movie_id", "genre_id"},
            }
            counts = {}
            for table, columns in required.items():
                actual = set(schema.loc[schema.table_name == table, "column_name"])
                if not columns <= actual:
                    raise ValueError(f"Schema {table} thiếu: {sorted(columns - actual)}")
                counts[table] = int(conn.execute(text(f"SELECT count(*) FROM {table}")).scalar())
                if not counts[table]:
                    raise ValueError(f"Bảng {table} đang trống.")
            # Chặn dataset vượt ngân sách trước khi tải; không lấy mẫu âm thầm.
            estimated_bytes = counts["fact_rating"] * 256 + counts["dim_movie"] * 8192
            budget = int(os.getenv("ML_MEMORY_BUDGET_MB", "1024")) * 1024**2
            if estimated_bytes > budget:
                raise ValueError("Dataset vượt ước lượng ngân sách RAM; cần đánh giá tài nguyên trước.")
            orphan = conn.execute(text("""
                SELECT count(*) FROM fact_rating f
                LEFT JOIN dim_movie m ON f.movie_id=m.movie_id
                LEFT JOIN dim_user u ON f.user_id=u.user_id
                LEFT JOIN dim_time t ON f.time_id=t.time_id
                WHERE m.movie_id IS NULL OR u.user_id IS NULL OR t.time_id IS NULL
            """)).scalar()
            if orphan:
                raise ValueError(f"Warehouse có {orphan} rating thiếu khóa tham chiếu.")
            ratings = pd.read_sql_query(text("""
                SELECT f.user_id, f.movie_id, f.rating
                FROM fact_rating f JOIN dim_movie m ON f.movie_id=m.movie_id
                JOIN dim_user u ON f.user_id=u.user_id
                ORDER BY f.user_id, f.movie_id
            """), conn)
            movies = pd.read_sql_query(text("""
                SELECT movie_id, title, genres, overview, has_tmdb_metadata
                FROM dim_movie ORDER BY movie_id
            """), conn)
    finally:
        engine.dispose()
    if len(ratings) != counts["fact_rating"] or ratings.duplicated(["user_id", "movie_id"]).any():
        raise ValueError("Rating bị mất hoặc nhân dòng khi JOIN, hoặc trùng user–movie.")
    if ratings.isna().any().any() or not ratings.rating.between(0.5, 5).all():
        raise ValueError("Rating/khóa null hoặc rating không hợp lệ.")
    if movies.movie_id.duplicated().any() or movies[["movie_id", "title"]].isna().any().any():
        raise ValueError("Khóa phim trùng/null hoặc title null.")
    movies["genres"] = movies.genres.fillna("")
    fingerprint = hashlib.sha256(pd.util.hash_pandas_object(ratings, index=False).values.tobytes()
                                 + pd.util.hash_pandas_object(movies, index=False).values.tobytes()).hexdigest()
    source = {"tables": counts, "users_with_ratings": int(ratings.user_id.nunique()),
              "rated_movies": int(ratings.movie_id.nunique()),
              "tmdb_movies": int(movies.has_tmdb_metadata.sum()),
              "snapshot_sha256": fingerprint, "schema": schema.to_dict("records")}
    print("Warehouse READ ONLY:", json.dumps({k: v for k, v in source.items() if k != "schema"}))
    return ratings, movies, source


def split_positives(positive, seed):
    """Holdout 20% theo user, giữ ít nhất 1 positive train/user đánh giá."""
    rng = np.random.default_rng(seed)
    test_indices = []
    for _, group in positive.groupby("user_id", sort=True):
        if len(group) >= 2:
            size = max(1, int(np.floor(len(group) * 0.2)))
            test_indices.extend(rng.choice(group.index.to_numpy(), size=size, replace=False))
    test = positive.loc[sorted(test_indices)].copy()
    train = positive.drop(index=test_indices).copy()
    train_pairs = set(zip(train.user_id, train.movie_id))
    test_pairs = set(zip(test.user_id, test.movie_id))
    if not test_pairs or train_pairs & test_pairs:
        raise ValueError("Split trống hoặc bị rò rỉ cặp train/test.")
    if not set(test.user_id) <= set(train.user_id):
        raise ValueError("User test không có lịch sử train.")
    return train, test


def make_features(movies, train):
    """Genres MovieLens + token overview TMDB thực; từ vựng chỉ fit trên train."""
    docs = movies.overview.fillna("").where(movies.has_tmdb_metadata, "")
    docs = docs.mask(docs.str.strip().eq("No overview available"), "")
    train_docs = docs[movies.movie_id.isin(train.movie_id.unique()) & docs.str.strip().ne("")]
    vectorizer = CountVectorizer(stop_words="english", min_df=2, max_features=300, binary=True)
    if len(train_docs) >= 2:
        try:
            vectorizer.fit(train_docs)
        except ValueError as error:
            raise ValueError("Không tạo được vocabulary overview từ dữ liệu train.") from error
        words = vectorizer.get_feature_names_out()
        text_features = vectorizer.transform(docs).tocsr()
    else:
        words, text_features = [], None
    rows = []
    for i, movie in enumerate(movies.itertuples(index=False)):
        features = ["genre:" + g.strip() for g in (movie.genres or "").split("|")
                    if g.strip() and g.strip() != "(no genres listed)"]
        if text_features is not None:
            features.extend("overview:" + words[j] for j in text_features[i].indices)
        rows.append((int(movie.movie_id), features))
    dataset = Dataset()  # item identity được giữ cùng với metadata.
    users = sorted(train.user_id.unique().tolist())
    dataset.fit(users, movies.movie_id.tolist(),
                item_features=sorted({f for _, features in rows for f in features}))
    item_features = dataset.build_item_features(rows)
    user_map, _, item_map, _ = dataset.mapping()
    return item_features, user_map, item_map, len(words)


def interactions(frame, user_map, item_map):
    return coo_matrix((np.ones(len(frame), dtype=np.float32),
                       ([user_map[u] for u in frame.user_id],
                        [item_map[m] for m in frame.movie_id])),
                      shape=(len(user_map), len(item_map))).tocsr()


def popularity(frame, item_map):
    counts = frame.groupby("movie_id").user_id.nunique()
    return np.array([counts.get(m, 0) for m in item_map], dtype=np.float32)


def ranked_items(scores, movie_ids, excluded, k):
    # Hòa điểm được xử lý xác định theo movie_id cho cả hai model.
    eligible = np.flatnonzero(~np.isin(movie_ids, list(excluded)))
    order = np.lexsort((movie_ids[eligible], -scores[eligible]))
    return movie_ids[eligible[order[:k]]]


def ranking_metrics(recommended, relevant, k=10):
    hits = np.array([m in relevant for m in recommended], dtype=float)
    dcg = (hits / np.log2(np.arange(len(hits)) + 2)).sum()
    ideal = (1 / np.log2(np.arange(min(k, len(relevant))) + 2)).sum()
    return np.array([hits.sum() / k, hits.sum() / len(relevant), dcg / ideal])


def evaluate(bundle):
    train, test, ratings = bundle["train"], bundle["test"], bundle["ratings"]
    movie_ids = bundle["movies"].movie_id.to_numpy()
    train_seen = train.groupby("user_id").movie_id.agg(set).to_dict()
    low_seen = ratings[ratings.rating < 4].groupby("user_id").movie_id.agg(set).to_dict()
    popular = popularity(train, bundle["item_map"])
    metrics = {"Most Popular": [], "LightFM + WARP": []}
    item_indices = np.arange(len(movie_ids), dtype=np.int32)
    for user_id, group in test.groupby("user_id", sort=True):
        relevant = set(group.movie_id)
        # Không lọc positive test ở bước offline evaluation.
        excluded = train_seen.get(user_id, set()) | low_seen.get(user_id, set())
        assert not relevant & excluded
        scores = bundle["evaluation_model"].predict(bundle["user_map"][user_id], item_indices,
                                                    item_features=bundle["features"], num_threads=1)
        for name, values in [("Most Popular", popular), ("LightFM + WARP", scores)]:
            top = ranked_items(values, movie_ids, excluded, 10)
            metrics[name].append(ranking_metrics(top, relevant))
    result = {}
    for name, values in metrics.items():
        result[name] = dict(zip(["Precision@10", "Recall@10", "NDCG@10"],
                               np.mean(values, axis=0).tolist()))
    return result


def train_models(args):
    ratings, movies, source = read_warehouse()
    positive = ratings[ratings.rating >= 4].copy()
    train, test = split_positives(positive, args.seed)
    # Mapping chứa user có positive; user khác được xử lý bằng fallback khi demo.
    features, user_map, item_map, vocabulary_size = make_features(movies, train)
    settings = dict(loss="warp", no_components=32, random_state=args.seed)
    evaluation_model = LightFM(**settings)
    evaluation_model.fit(interactions(train, user_map, item_map), item_features=features,
                         epochs=args.epochs, num_threads=1)
    bundle = dict(ratings=ratings, movies=movies, source=source, train=train, test=test,
                  features=features, user_map=user_map, item_map=item_map,
                  evaluation_model=evaluation_model)
    result = evaluate(bundle)
    report = {"source": source, "seed": args.seed, "epochs": args.epochs,
              "components": 32, "threads": 1, "overview_vocabulary": vocabulary_size,
              "positive_interactions": len(positive), "train_interactions": len(train),
              "test_interactions": len(test), "evaluation_users": int(test.user_id.nunique()),
              "train_test_overlap": 0, "test_users_without_train": 0,
              "metrics": result}
    print(json.dumps({k: v for k, v in report.items() if k != "source"}, indent=2))
    # Model demo refit toàn bộ positive, tách biệt model dùng báo cáo test.
    model = LightFM(**settings)
    model.fit(interactions(positive, user_map, item_map), item_features=features,
              epochs=args.epochs, num_threads=1)
    bundle.update(model=model, report=report, popular=popularity(positive, item_map))
    ARTIFACTS.mkdir(exist_ok=True)
    # Ghi file tạm để demo vẫn giữ được model cũ nếu quá trình lưu bị lỗi.
    model_temp = ARTIFACTS / "model.pkl.tmp"
    with model_temp.open("wb") as out:
        pickle.dump(bundle, out, protocol=pickle.HIGHEST_PROTOCOL)
    metrics_temp = ARTIFACTS / "metrics.json.tmp"
    metrics_temp.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(model_temp, ARTIFACTS / "model.pkl")
    os.replace(metrics_temp, ARTIFACTS / "metrics.json")
    print("Đã lưu model đánh giá, model demo refit và metrics trong ml_artifacts/.")


def recommend(bundle, user_id, k):
    ratings, movies = bundle["ratings"], bundle["movies"]
    history = ratings[ratings.user_id == user_id]
    fallback = user_id not in bundle["user_map"]
    if fallback:
        reason = "User không tồn tại" if history.empty else "User chưa có tương tác tích cực"
        print(reason + "; dùng Most Popular (toàn bộ lịch sử đã nạp).")
        scores = bundle["popular"]
    else:
        scores = bundle["model"].predict(bundle["user_map"][user_id],
                  np.arange(len(movies), dtype=np.int32), item_features=bundle["features"], num_threads=1)
    top = ranked_items(scores, movies.movie_id.to_numpy(), set(history.movie_id), k)
    assert not set(top) & set(history.movie_id)
    return [(int(m), movies.iloc[bundle["item_map"][m]].title,
             float(scores[bundle["item_map"][m]])) for m in top]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--train", action="store_true")
    mode.add_argument("--evaluate", action="store_true")
    mode.add_argument("--user-id", type=int)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--epochs", type=int, default=30)
    args = parser.parse_args()
    if args.top_k < 1 or args.epochs < 1:
        parser.error("top-k và epochs phải dương.")
    try:
        if args.train:
            train_models(args)
        else:
            # Chỉ load pickle tự tạo tại máy này; không dùng artifact không rõ nguồn.
            with (ARTIFACTS / "model.pkl").open("rb") as inp:
                bundle = pickle.load(inp)
            if args.evaluate:
                print(json.dumps(evaluate(bundle), indent=2))
            else:
                print(f"User ID: {args.user_id}\nTop {args.top_k} Recommendations:")
                for rank, (movie_id, title, score) in enumerate(recommend(bundle, args.user_id, args.top_k), 1):
                    print(f"{rank:2}. {title} [movie_id={movie_id}]  Score: {score:.6f}")
                print("Score là điểm xếp hạng; fallback Most Popular dùng số user tích cực.")
    except (ValueError, FileNotFoundError) as error:
        parser.exit(1, f"Lỗi: {error}\n")
    except SQLAlchemyError:
        parser.exit(1, "Lỗi kết nối/truy vấn Warehouse. Kiểm tra DW_URL và schema.\n"
                    "Nếu dùng PostgreSQL local, khởi động bằng: docker compose up -d --wait postgres-dw\n"
                    "--evaluate và --user-id đọc model đã lưu nên không cần kết nối Warehouse.\n")


if __name__ == "__main__":
    main()
