import pandas as pd


def fetch_data(**kwargs):
    file_path = '/home/nicus/projects/challenges/airflow/Data_Train.xlsx'
    df = pd.read_excel(file_path)


    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())