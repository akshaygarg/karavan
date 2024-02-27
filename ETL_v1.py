import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
import json



default_args = {
    'owner': 'akshay',
    'depends_on_past': False,
    'start_date': datetime(2017, 10, 9),
    'email': 'akshay.garg@atoss.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('Read_Data',
    schedule_interval='@once',
    default_args=default_args)

t1 = SimpleHttpOperator(
    task_id='get_labrador',
    method='GET',
    http_conn_id='northwind',
    endpoint='northwind/northwind.svc/Customers?$format=json',
    headers={"Content-Type": "application/json"},
    dag=dag)

t2 = SimpleHttpOperator(
    task_id='get_breeds',
    method='GET',
    http_conn_id='http_default',
    endpoint='api/breeds/list',
    headers={"Content-Type": "application/json"},
    dag=dag)
    
t1 >> t1
