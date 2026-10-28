"""Web demo local: điều phối pipeline hiện có và dùng model thật đã lưu."""
import csv
import io
import json
import os
import pickle
import re
import subprocess
import sys
import threading
import uuid
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
ARTIFACTS = ROOT / "ml_artifacts"
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 4096
app.json.ensure_ascii = False
guard = threading.RLock()
job = {"status": "idle", "steps": []}
cache = {"signature": None, "bundle": None}
TABLES = ("dim_user", "dim_movie", "dim_genre", "bridge_movie_genre", "dim_time", "fact_rating")


def now():
    return datetime.now(timezone.utc).isoformat()


def safe_text(value):
    """Không đưa mật khẩu/URL kết nối vào log trình duyệt."""
    value = str(value)
    for key, secret in os.environ.items():
        if any(word in key.upper() for word in ("PASSWORD", "SECRET", "API_KEY", "DW_URL")) and len(secret) >= 4:
            value = value.replace(secret, "[ẩn]")
    return re.sub(r"postgres(?:ql)?(?:\+\w+)?://\S+", "[database URL ẩn]", value)


def raw_status():
    rows = []
    for name in ("movies.csv", "ratings.csv", "links.csv", "tags.csv", "tmdb_movies_raw.json"):
        p = ROOT / "raw_data" / name
        rows.append({"file": name, "exists": p.is_file(), "bytes": p.stat().st_size if p.is_file() else 0})
    return rows


def check_raw():
    """Đọc nguồn thực, không huấn luyện từ raw."""
    if not all(r["exists"] for r in raw_status()):
        raise ValueError("Thiếu file trong raw_data/. Hãy bổ sung nguồn trước khi chạy ETL.")
    counts = {}
    required = {"movies.csv": {"movieId", "title", "genres"},
                "ratings.csv": {"userId", "movieId", "rating", "timestamp"},
                "links.csv": {"movieId", "tmdbId"}, "tags.csv": {"userId", "movieId", "tag"}}
    for name, columns in required.items():
        with (ROOT / "raw_data" / name).open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not columns <= set(reader.fieldnames or []):
                raise ValueError(f"Nguồn {name} thiếu cột cần thiết.")
            counts[name] = sum(1 for _ in reader)
    data = json.loads((ROOT / "raw_data/tmdb_movies_raw.json").read_text())
    if not isinstance(data, list) or not data or any(not isinstance(r, dict) or "tmdb_id" not in r for r in data):
        raise ValueError("TMDB JSON không hợp lệ hoặc rỗng.")
    counts["tmdb_movies_raw.json"] = len(data)
    if not all(counts.values()):
        raise ValueError("Nguồn dữ liệu có file rỗng.")
    return counts


def warehouse_status():
    url = os.getenv("DW_URL")
    if not url:
        return {"connected": False, "ready": False, "message": "Chưa cấu hình DW_URL trong .env."}
    engine = None
    try:
        engine = create_engine(url, connect_args={"connect_timeout": 3})
        with engine.connect() as conn, conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL statement_timeout = '5s'"))
            available = set(conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'")).scalars())
            counts = {t: int(conn.execute(text(f'SELECT count(*) FROM "{t}"')).scalar())
                      for t in TABLES if t in available}
            ready = len(counts) == len(TABLES) and all(counts.values())
        return {"connected": True, "ready": bool(ready), "counts": counts,
                "message": "Có đủ 6 bảng và dữ liệu. Train sẽ kiểm định thêm." if ready else "Database đã kết nối; cần chạy ETL để nạp đủ bảng."}
    except Exception:
        return {"connected": False, "ready": False,
                "message": "Chưa kết nối được PostgreSQL. Bấm Khởi động DB hoặc kiểm tra DW_URL."}
    finally:
        if engine is not None:
            engine.dispose()


def load_bundle():
    p = ARTIFACTS / "model.pkl"
    if not p.is_file():
        raise ValueError("Chưa có model. Chạy pipeline hoặc huấn luyện trước.")
    signature = (p.stat().st_mtime_ns, p.stat().st_size)
    if signature != cache["signature"]:
        # Chỉ load artifact tự tạo của dự án, không nhận upload pickle từ web.
        with p.open("rb") as f:
            cache["bundle"] = pickle.load(f)
        cache["signature"] = signature
    return cache["bundle"]


def busy():
    return job["status"] == "running"


def log_message(path, message):
    with path.open("a", encoding="utf-8") as f:
        f.write(safe_text(message) + "\n")


def commands(action, epochs, seed):
    db = ["docker", "compose", "--project-directory", str(ROOT), "up", "-d", "--wait",
          "--wait-timeout", "90", "postgres-dw"]
    etl = [sys.executable, "-u", str(ROOT / "etl_process.py")]
    train = [sys.executable, "-u", str(ROOT / "recommend.py"), "--train", "--epochs", str(epochs), "--seed", str(seed)]
    evaluate = [sys.executable, "-u", str(ROOT / "recommend.py"), "--evaluate"]
    steps = {"check": [("Kiểm tra nguồn raw", None)], "database": [("Khởi động PostgreSQL", db)],
             "etl": [("Kiểm tra nguồn raw", None), ("ETL và kiểm định Warehouse", etl)],
             "train": [("Huấn luyện và lưu model", train)],
             "evaluate": [("Đánh giá lại model đã lưu", evaluate)]}
    steps["pipeline"] = steps["check"] + steps["database"] + steps["etl"][1:] + steps["train"] + steps["evaluate"]
    return steps[action]


def run_job(steps, path):
    env = dict(os.environ, RAW_STORAGE="local", DATA_DIR=str(ROOT / "raw_data"),
               STAGING_DIR=str(ARTIFACTS / "staging"), PYTHONUNBUFFERED="1")
    try:
        for index, (title, command) in enumerate(steps):
            with guard:
                job["steps"][index]["status"] = "running"
                job["current_step"] = title
            log_message(path, f"\n→ {title}")
            if command is None:
                log_message(path, json.dumps(check_raw(), ensure_ascii=False))
            else:
                # Danh sách lệnh cố định, không nhận shell command từ trình duyệt.
                with path.open("a", encoding="utf-8") as output:
                    process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output,
                                               stderr=subprocess.STDOUT)
                    try:
                        code = process.wait(timeout=1800)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                        raise ValueError("Tác vụ vượt thời gian 30 phút; đã dừng tiến trình.")
                if code:
                    raise ValueError(f"{title} thất bại (exit {code}). Xem log bên dưới.")
            with guard:
                job["steps"][index]["status"] = "complete"
        with guard:
            job.update(status="complete", message="Hoàn thành tất cả bước được yêu cầu.", finished_at=now())
            cache["signature"] = None
    except Exception as error:
        log_message(path, f"LỖI: {safe_text(error)}")
        with guard:
            for step in job["steps"]:
                if step["status"] == "running":
                    step["status"] = "error"
            job.update(status="error", message=safe_text(error), finished_at=now())
    finally:
        with guard:
            (ARTIFACTS / "web_last_job.json").write_text(json.dumps(job, ensure_ascii=False, indent=2))


@app.before_request
def local_requests_only():
    # App quản trị local: chặn Host lạ và POST từ website khác.
    if request.host.split(":")[0] not in {"127.0.0.1", "localhost"}:
        return jsonify(error="Chỉ hỗ trợ truy cập local."), 403
    if request.method == "POST" and request.headers.get("X-Demo-Request") != "1":
        return jsonify(error="Thiếu header thao tác của giao diện local."), 403


@app.get("/")
def home():
    return render_template("index.html")


@app.get("/api/status")
def status():
    model = {"available": (ARTIFACTS / "model.pkl").is_file()}
    with guard:
        running = busy()
        if model["available"] and not running:
            try:
                bundle = load_bundle()
                model.update(report=bundle["report"], updated_at=datetime.fromtimestamp(
                    (ARTIFACTS / "model.pkl").stat().st_mtime, timezone.utc).isoformat())
            except Exception:
                model.update(available=False, message="Artifact không đọc được; cần train lại.")
    return jsonify(raw=raw_status(), warehouse=warehouse_status(), model=model, running=running)


@app.get("/api/job")
def job_status():
    with guard:
        result = dict(job)
        if job.get("log"):
            with (ARTIFACTS / job["log"]).open("rb") as f:
                f.seek(0, 2)
                f.seek(max(0, f.tell() - 64000))
                result["output"] = safe_text(f.read().decode("utf-8", errors="replace"))
        return jsonify(result)


@app.post("/api/jobs")
def start_job():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify(error="Yêu cầu JSON không hợp lệ."), 400
    action, epochs, seed = data.get("action"), data.get("epochs", 30), data.get("seed", 42)
    if not isinstance(action, str) or action not in {"pipeline", "check", "database", "etl", "train", "evaluate"}:
        return jsonify(error="Tác vụ không được hỗ trợ."), 400
    if type(epochs) is not int or not 1 <= epochs <= 100 or type(seed) is not int or not 0 <= seed <= 2147483647:
        return jsonify(error="Epochs từ 1–100; seed từ 0–2147483647, đều là số nguyên."), 400
    with guard:
        if busy():
            return jsonify(error="Đang có tác vụ chạy. Hãy chờ hoàn thành."), 409
        steps = commands(action, epochs, seed)
        ARTIFACTS.mkdir(exist_ok=True)
        filename = f"web_{uuid.uuid4().hex[:12]}.log"
        path = ARTIFACTS / filename
        path.touch(mode=0o600)
        job.clear()
        job.update(status="running", action=action, started_at=now(), log=filename,
                   steps=[{"title": title, "status": "pending"} for title, _ in steps])
        threading.Thread(target=run_job, args=(steps, path), daemon=True).start()
        return jsonify(job), 202


@app.get("/api/recommendations")
def recommendations():
    try:
        user_id, k = int(request.args.get("user_id", "15")), int(request.args.get("top_k", "10"))
        if not 1 <= user_id <= 2147483647 or not 1 <= k <= 20:
            raise ValueError("User ID phải dương, Top K từ 1 đến 20.")
        with guard:
            if busy():
                return jsonify(error="Chờ tác vụ hoàn thành trước khi tải model/demo."), 409
            bundle = load_bundle()
            from recommend import recommend
            with redirect_stdout(io.StringIO()):
                top = recommend(bundle, user_id, k)
            movies = bundle["movies"].set_index("movie_id")
            history = bundle["ratings"].query("user_id == @user_id")
            items = [{"movie_id": mid, "title": title, "score": score,
                      "genres": str(movies.loc[mid, "genres"] or "Chưa có thể loại")}
                     for mid, title, score in top]
            seen = set(history.movie_id)
            if seen & {r["movie_id"] for r in items}:
                raise ValueError("Kiểm tra lịch sử thất bại; không trả gợi ý trùng.")
            history_rows = [{"movie_id": int(r.movie_id), "title": str(movies.loc[r.movie_id, "title"]),
                             "rating": float(r.rating)} for r in history.itertuples()]
            return jsonify(user_id=user_id, known_user=not history.empty,
                           fallback=user_id not in bundle["user_map"], history_count=len(history),
                           positive_count=int((history.rating >= 4).sum()), history=history_rows,
                           recommendations=items, overlap=0, snapshot=bundle["source"]["snapshot_sha256"])
    except (ValueError, FileNotFoundError) as error:
        return jsonify(error=safe_text(error)), 400
    except Exception:
        return jsonify(error="Không đọc được model. Hãy kiểm tra môi trường hoặc train lại."), 500


if __name__ == "__main__":
    # Một process, không bật reloader để tránh tạo hai worker cùng chạy pipeline.
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
