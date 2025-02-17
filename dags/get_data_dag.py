from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.clean_data import clean_data
from tasks.fetch_data import fetch_data
from tasks.train_model import train_model

default_args = {
    'start_date': datetime(2024, 2, 17),
    'retries': 2
}

dag = DAG(
    'get_data',
    default_args=default_args,
    schedule_interval='@daily',
)

t1 = PythonOperator(task_id='fetch_data', python_callable=fetch_data, dag=dag)
t2 = PythonOperator(task_id='clean_data', python_callable=clean_data, dag=dag)
t3 = PythonOperator(task_id='train_model', python_callable=train_model, dag=dag)

t1 >> t2 >> t3