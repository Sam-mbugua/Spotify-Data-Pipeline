
from datetime import datetime
from airflow import DAG 
from airflow.operators.python_opera
from airflow.utils.dates import days_ago

from spotify_etl import run_spotify_etl

default_args = {
    'owner':'Sam Mbugua',
    'depends_on_past' : False
    'start_date' : datetime(2021,9,12)
    'email' : ['skm228@cornell.edu'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries': 1,
    'retry_delay' : timedelta(minute=1)
}

dag = DAG(
    'spotify_dag',
    default_args = default_args,
    description = 'First DAG with ETL process!',
    schedule_interval = timedelta(days=1),
)

run_etl = PythonOperator(
    task_id = 'whole_spotify_etl'
    python_callable = run_spotify_etl,
    dag = dag,
)

run_etl