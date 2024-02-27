from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['akshay.garg@atoss.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'Personio_Example',
    default_args=default_args,
    description='Call the personio API',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Integration'],
) as dag:
    t1 = SimpleHttpOperator(
                    task_id='Read_Data',
                    method='GET',
                    http_conn_id='northwind',
                    endpoint='northwind/northwind.svc/Customers?$format=json',
                    headers={"Content-Type": "application/json"},
                    dag=dag)
    
    def xcom_check(ds, **kwargs):
        val = kwargs['ti'].xcom_pull(key='return_value', task_ids='Read_Data')
        return f"xcom_check has: {kwargs['ti']} and it says: {val}"
     
    t2 = PythonOperator(
        task_id='inspect_dog',
        python_callable=xcom_check,
        provide_context=True
    )

t1 >> t2
