import sys
import os
from datetime import datetime, timedelta
import pendulum
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
    load_fact_rating,
    validate_data_warehouse
)
from ingest_to_minio import upload_raw_to_minio

default_args = {
    'owner': 'aizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1, tzinfo=pendulum.timezone('Asia/Ho_Chi_Minh')),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='movielens_data_warehouse_etl',
    default_args=default_args,
    description='Tự động trích xuất từ MinIO, làm sạch, nạp vào Data Warehouse Star Schema & kiểm tra chất lượng tự động',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
) as dag:

    # Upload current raw_data snapshots to MinIO before the ETL reads them.
    task_ingest = PythonOperator(
        task_id='ingest_raw_to_minio',
        python_callable=upload_raw_to_minio,
    )

    # Task 1: Trích xuất dữ liệu thô từ MinIO Data Lake
    task_extract = PythonOperator(
        task_id='extract_from_minio',
        python_callable=extract_data
    )

    # Task 2: Làm sạch, khử trùng lặp và chuẩn hóa dữ liệu (Star Schema + Bridge)
    task_transform = PythonOperator(
        task_id='clean_and_transform',
        python_callable=transform_data
    )

    # Task 3: Khởi tạo schema PK/FK và nạp các bảng Dimension + Bridge
    task_load_dims = PythonOperator(
        task_id='load_dimension_tables',
        python_callable=load_dimension_tables
    )

    # Task 4: Nạp bảng Fact Rating vào PostgreSQL DW
    task_load_fact = PythonOperator(
        task_id='load_fact_rating',
        python_callable=load_fact_rating
    )

    # Task 5: Kiểm định chất lượng dữ liệu tự động (Data Quality Check)
    task_validate = PythonOperator(
        task_id='validate_data_warehouse',
        python_callable=validate_data_warehouse
    )

    # Pipeline: Thứ tự thực thi chuẩn ETL Data Warehouse
    task_ingest >> task_extract >> task_transform >> task_load_dims >> task_load_fact >> task_validate
