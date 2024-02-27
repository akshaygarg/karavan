import json

import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 27),
    'email': ['ss@asd.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}
dag = DAG(
    'dag1',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
)
t2 = SimpleHttpOperator(
    task_id='get_labrador',
    method='GET',
    http_conn_id='northwind',
    endpoint='northwind/northwind.svc/Customers?$format=json',
    headers={"Content-Type": "application/json"},
    xcom_push=True,
    dag=dag
)


