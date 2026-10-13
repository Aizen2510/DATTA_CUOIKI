#!/bin/bash
# ==============================================================
# Script khởi chạy Apache Airflow Webserver + Scheduler (Gọn nhẹ)
# ==============================================================
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

source venv/bin/activate
export AIRFLOW_HOME="$DIR/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "--------------------------------------------------------"
echo " Khởi chạy Apache Airflow Standalone..."
echo " Web UI   : http://localhost:8080"
echo " Username : admin"
echo " Password : admin"
echo "--------------------------------------------------------"

airflow standalone
