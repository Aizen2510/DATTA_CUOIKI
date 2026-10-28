@echo off
setlocal
cd /d "%~dp0"

if not exist ".env" (
    echo [ERROR] Thieu file .env! Vui long kiem tra file .env truoc.
    exit /b 1
)

echo Dang khoi dong cac dich vu Docker...
docker compose up -d

echo ====================================================
echo Cac dich vu da san sang:
echo   - Airflow UI:      http://localhost:8080 (airflow / airflow)
echo   - MinIO Console:   http://localhost:9001 (minioadmin / minioadmin)
echo   - PostgreSQL DW:   localhost:5434 (dw_movies)
echo ====================================================
