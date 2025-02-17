from datetime import datetime

import joblib
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder, MinMaxScaler

from helpers.helpers import convert_duration_to_minutes, convert_datetime_to_minutes, convert_Total_Stops

defaults_args = dict(
    owner='airflow',
    start_date=datetime(2025,2,14),
    retries=1,
)

dag = DAG(
    'get_data',
    default_args=defaults_args,
    description='Get information from DB',
    schedule_interval='@daily',
    catchup=False
)

def read_csv_file():
    file_path = '/home/nicus/projects/challenges/airflow/Data_Train.xlsx'
    data = pd.read_excel(file_path)
    df = pd.DataFrame(data)
    df["Date_of_Journey"] = pd.to_datetime(df["Date_of_Journey"])
    df["Day"] = df["Date_of_Journey"].dt.day
    df["Month"] = df["Date_of_Journey"].dt.month
    df["Year"] = df["Date_of_Journey"].dt.year

    df['Duration'] = df['Duration'].apply(convert_duration_to_minutes)

    Q1 = df["Duration"].quantile(0.25)
    Q3 = df["Duration"].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = df[(df['Duration'] < lower_bound) | (df["Duration"] > upper_bound)]
    df = df.drop(outliers.index)

    df["Total_Stops"] = df["Total_Stops"].fillna("0 stops")

    df["Total_Stops"] = df["Total_Stops"].apply(convert_Total_Stops)

    scaler = MinMaxScaler()
    df[["Duration"]] = scaler.fit_transform(df[["Duration"]])

    def convert_to_minutes(time):
        hours, minutes = map(int, time.split(":"))
        return minutes + hours * 60

    df["Dep_Time"] = df['Dep_Time'].apply(convert_to_minutes)
    df[["Dep_Time"]] = scaler.fit_transform(df[["Dep_Time"]])


    df['Arrival_Time'] = df['Arrival_Time'].apply(convert_datetime_to_minutes)

    df.dropna(subset=['Arrival_Time'], inplace=True)

    df[['Arrival_Time']] = scaler.fit_transform(df[['Arrival_Time']])
    label_encoder = LabelEncoder()

    df['Source'] = label_encoder.fit_transform(df['Source'])
    df['Destination'] = label_encoder.fit_transform(df['Destination'])
    df['Additional_Info'] = label_encoder.fit_transform(df['Additional_Info'])

    # Train model
    X_train = df[
        ['Duration', 'Total_Stops', 'Source', 'Destination', 'Additional_Info', 'Dep_Time', 'Arrival_Time', 'Day',
         'Month', 'Year']]
    y_train = df['Price']

    params = {
        'n_estimators': 200,
        'learning_rate': 0.5,
        'max_depth': 6,
        'min_child_weight': 10,
        'gamma': 10,
        'subsample': 1,
        'colsample_bytree': 1,
        'reg_alpha': 10,
        'reg_lambda': 10
    }
    model = xgb.XGBRegressor(**params)

    model.fit(X_train, y_train)

    # save model
    with open('models/price_predictor.pkl', 'wb') as f:
        joblib.dump(model, f)

task_read = PythonOperator(
    task_id='read_csv_file',
    python_callable=read_csv_file,
    dag=dag
)

