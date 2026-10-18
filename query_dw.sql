-- 1. Xem 10 phim mới nhất và thông tin thể loại
SELECT 
    movie_id,
    title,
    release_year,
    genres,
    popularity,
    vote_average,
    has_tmdb_metadata
FROM dim_movie
ORDER BY movie_id
LIMIT 10;

-- 2. Thống kê tổng quan số lượng bản ghi của toàn bộ Data Warehouse
SELECT 'dim_user' AS table_name, count(*) AS total_rows FROM dim_user
UNION ALL
SELECT 'dim_movie', count(*) FROM dim_movie
UNION ALL
SELECT 'dim_genre', count(*) FROM dim_genre
UNION ALL
SELECT 'bridge_movie_genre', count(*) FROM bridge_movie_genre
UNION ALL
SELECT 'dim_time', count(*) FROM dim_time
UNION ALL
SELECT 'fact_rating', count(*) FROM fact_rating;

-- 3. Top 10 phim được đánh giá cao nhất (có ít nhất 50 lượt rating)
SELECT 
    m.title,
    m.release_year,
    ROUND(AVG(f.rating)::numeric, 2) AS avg_rating,
    COUNT(f.rating) AS total_ratings
FROM fact_rating f
JOIN dim_movie m ON f.movie_id = m.movie_id
GROUP BY m.movie_id, m.title, m.release_year
HAVING COUNT(f.rating) >= 50
ORDER BY avg_rating DESC, total_ratings DESC
LIMIT 10;

-- 4. Thống kê số lượng phim theo từng thể loại qua bảng Bridge (Chuẩn Star Schema)
SELECT 
    g.genre_name,
    COUNT(b.movie_id) AS total_movies
FROM dim_genre g
JOIN bridge_movie_genre b ON g.genre_id = b.genre_id
GROUP BY g.genre_id, g.genre_name
ORDER BY total_movies DESC;

-- 5. Xem 10 lượt đánh giá mới nhất kèm thời gian chi tiết
SELECT 
    f.user_id,
    m.title,
    f.rating,
    f.rating_timestamp,
    t.hour,
    t.day,
    t.month,
    t.year,
    t.day_name
FROM fact_rating f
JOIN dim_movie m ON f.movie_id = m.movie_id
JOIN dim_time t ON f.time_id = t.time_id
ORDER BY f.rating_timestamp DESC
LIMIT 10;
