from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from mekarde.module.etl_file import file_to_db
from mekarde.module.etl_db import replicate

default_args = {
    'owner': 'dinda',
    'start_date': datetime(2024, 8, 22),
    'retries': 1
}

dag = DAG(
    'data_001_transactions',
    default_args=default_args,
    description='Job to replicate transactions data',
    schedule_interval='0 */2 * * *',
    catchup=False
)


def run_file_to_staging():
    file_to_db('s3://data.lake.mekarde2/externals/[Confidential] Mekari - Data Engineer Senior.xlsx',
               'staging',
               'transactions')


def run_staging_to_dwh():
    replicate('dwh_transactions_data.sql',
              'datawarehouse',
              'transactions',
              'dwh_transactions_table.sql')


file_to_staging = PythonOperator(
    task_id='file_to_staging',
    python_callable=run_file_to_staging,
    dag=dag,
)

staging_to_dwh = PythonOperator(
    task_id='staging_to_dwh',
    python_callable=run_staging_to_dwh,
    dag=dag,
)

file_to_staging >> staging_to_dwh
