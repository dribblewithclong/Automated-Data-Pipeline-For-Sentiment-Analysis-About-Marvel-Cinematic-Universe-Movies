from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl_dag import run_reddit_etl

#------------------------------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 27),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'reddit_dag',
    default_args=default_args,
    description='DAG with ETL process!',
    schedule=timedelta(days=7),
)

run_etl = PythonOperator(
    task_id='whole_reddit_etl',
    python_callable=run_reddit_etl,
    dag=dag,
)

run_etl