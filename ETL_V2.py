from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator

def extract():
    # Your extract logic here
    t1 = SimpleHttpOperator(
    task_id='Read_Data',
    method='GET',
    http_conn_id='northwind',
    endpoint='northwind/northwind.svc/Customers?$format=json',
    headers={"Content-Type": "application/json"},
    dag=dag)

    t1()
    pass

def transform():
    # Your transform logic here
    pass

def load():
    # Your load logic here
    pass

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG('etl_v2_pipeline', default_args=default_args, schedule=None)

extract_task = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
load_task = PythonOperator(task_id='load', python_callable=load, dag=dag)

extract_task >> transform_task >> load_task
