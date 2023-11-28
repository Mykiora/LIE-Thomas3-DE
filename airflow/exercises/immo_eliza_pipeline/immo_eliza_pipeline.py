from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 23, 11),
}

immo_eliza_pipeline = DAG(
    dag_id="immo_eliza_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)
