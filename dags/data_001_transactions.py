from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from mekarde.module.etl_file import file_to_db

default_args = {
    'owner': 'dinda',
    'start_date': datetime(2024, 8, 22),
    'retries': 1,
}

dag = DAG(
    'data_001_transactions',
    default_args=default_args,
    description='Job to replicate transactions data',
    schedule_interval='0 */2 * * *',
)


def run_file_to_db():
    file_to_db('s3://data.lake.mekarde2/externals/[Confidential] Mekari - Data Engineer Senior.xlsx',
               'staging',
               'transactions')


hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=run_file_to_db,
    dag=dag,
)
