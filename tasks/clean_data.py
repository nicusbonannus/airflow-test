import pandas as pd
from sklearn.preprocessing import MinMaxScaler, LabelEncoder

from helpers.helpers import convert_duration_to_minutes, convert_total_stops, convert_datetime_to_minutes, \
    convert_to_minutes


def clean_data(**kwargs):
    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(task_ids='fetch_data', key='raw_data'))
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

    df["Total_Stops"] = df["Total_Stops"].apply(convert_total_stops)

    scaler = MinMaxScaler()
    df[["Duration"]] = scaler.fit_transform(df[["Duration"]])


    df["Dep_Time"] = df['Dep_Time'].apply(convert_to_minutes)
    df[["Dep_Time"]] = scaler.fit_transform(df[["Dep_Time"]])

    df['Arrival_Time'] = df['Arrival_Time'].apply(convert_datetime_to_minutes)

    df = df.dropna(subset=['Arrival_Time'])

    df[['Arrival_Time']] = scaler.fit_transform(df[['Arrival_Time']])
    label_encoder = LabelEncoder()

    df['Source'] = label_encoder.fit_transform(df['Source'])
    df['Destination'] = label_encoder.fit_transform(df['Destination'])
    df['Additional_Info'] = label_encoder.fit_transform(df['Additional_Info'])

    kwargs['ti'].xcom_push(key='clean_data', value=df.to_json())