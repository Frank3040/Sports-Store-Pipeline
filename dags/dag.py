import sys
from pathlib import Path
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from src.load_task import load_data
from src.transformation_task import transform_data

# Define paths
SCRIPT_PATH = Path(__file__).resolve()
DAGS_FOLDER = SCRIPT_PATH.parent
PROJECT_ROOT = DAGS_FOLDER

DATA_RAW_DIR = DAGS_FOLDER / "data" / "raw"

FILENAME = "sport_products_sales_analysis_challenge_raw.xlsx"
FILE_PATH = DATA_RAW_DIR / FILENAME

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

# Define DAG
dag = DAG(
    'sports_store_pipeline',
    default_args=default_args,
    description='Pipeline for Sports Store Sales Analysis',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['pipeline', 'sports_store'],
)

# Task 1: Check if the dataset exists
check_file_sensor = FileSensor(
    task_id='file_existance',
    filepath=str(FILE_PATH),
    poke_interval=30,
    timeout=600,
    mode='poke',
    dag=dag,
)

# Task 2: Preprocessing
load_data_task = PythonOperator(
    task_id='loading_data',
    python_callable=load_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transforming_data',
    python_callable=transform_data,
    dag=dag,
)

# Dependencies
check_file_sensor >> load_data_task >> transform_data_task