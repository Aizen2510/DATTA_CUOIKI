@echo off
setlocal

set ARTIFACTS_DIR=%~dp0ml_artifacts
if not exist "%ARTIFACTS_DIR%" mkdir "%ARTIFACTS_DIR%"

if "%1"=="--train" goto train
if "%1"=="-train" goto train
if "%1"=="train" goto train

if "%1"=="--evaluate" goto evaluate
if "%1"=="-evaluate" goto evaluate
if "%1"=="evaluate" goto evaluate

if "%1"=="--user-id" goto recommend
if "%1"=="-userId" goto recommend
if "%1"=="-user-id" goto recommend
if "%1"=="user" goto recommend

echo ====================================================
echo CACH SU DUNG:
echo   1. Train model:      run_ml.bat -train
echo   2. Danh gia model:   run_ml.bat -evaluate
echo   3. Goi y user 15:    run_ml.bat -userId 15 10
echo ====================================================
exit /b 0

:train
echo [ML] Dang train LightFM + WARP model tu PostgreSQL Data Warehouse...
docker run --rm --network datta_cuoiki_default -e DW_URL="postgresql+psycopg2://dw_user:dw_password@postgres_dw:5432/dw_movies" -v "%ARTIFACTS_DIR%:/app/ml_artifacts" movielens-ml --train
exit /b 0

:evaluate
echo [ML] Danh gia model offline trong ml_artifacts/...
docker run --rm -v "%ARTIFACTS_DIR%:/app/ml_artifacts" movielens-ml --evaluate
exit /b 0

:recommend
set USER_ID=%2
if "%USER_ID%"=="" set USER_ID=15
set TOP_K=%3
if "%TOP_K%"=="" set TOP_K=10
echo [ML] Goi y Top %TOP_K% phim cho User %USER_ID%...
docker run --rm -v "%ARTIFACTS_DIR%:/app/ml_artifacts" movielens-ml --user-id %USER_ID% --top-k %TOP_K%
exit /b 0
