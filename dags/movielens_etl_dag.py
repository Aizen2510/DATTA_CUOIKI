import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Thêm đường dẫn gốc của project vào sys.path để import được module etl_process
PROJECT_DIR = os.getenv("PROJECT_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from etl_process import (
    extract_data,
    transform_data,
    load_dimension_tables,
    load_fact_rating
)

default_args = {
    'owner': 'aizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='movielens_data_warehouse_etl',
    default_args=default_args,
    description='Tự động trích xuất từ MinIO, làm sạch và nạp vào Data Warehouse theo Star Schema',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Trích xuất dữ liệu thô từ MinIO Data Lake
    task_extract = PythonOperator(
        task_id='extract_from_minio',
        python_callable=extract_data
    )

    # Task 2: Làm sạch, khử trùng lặp và chuẩn hóa dữ liệu
    task_transform = PythonOperator(
        task_id='clean_and_transform',
        python_callable=transform_data
    )

    # Task 3: Nạp các bảng Dimension vào PostgreSQL DW
    task_load_dims = PythonOperator(
        task_id='load_dimension_tables',
        python_callable=load_dimension_tables
    )

    # Task 4: Nạp bảng Fact vào PostgreSQL DW
    task_load_fact = PythonOperator(
        task_id='load_fact_rating',
        python_callable=load_fact_rating
    )

    # Pipeline: Thứ tự thực thi chuẩn ETL
    task_extract >> task_transform >> task_load_dims >> task_load_fact
