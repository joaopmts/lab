from airflow import DAG
import pendulum
from airflow.macros import ds_add
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import os
import requests
from datetime import datetime, timedelta

def data_extract(**context):
    today = datetime.utcnow().date()
    data_interval_end = context['data_interval_end'].date()

    start_date = data_interval_end
    end_date = start_date + timedelta(days=6)

    if end_date >= today:
        start_date = start_date - timedelta(days=7)
        end_date = start_date + timedelta(days=6)

    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    lat = 42.3601
    lon = -71.0589

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_str,
        "end_date": end_str,
        "daily": "temperature_2m_min,temperature_2m_mean,temperature_2m_max,weathercode",
        "timezone": "America/New_York"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data_json = response.json()["daily"]
    df = pd.DataFrame(data_json)

    folder_path = f'/opt/airflow/data/climate_data_project/week{start_str}/'
    os.makedirs(folder_path, exist_ok=True)

    df.to_csv(os.path.join(folder_path, 'raw_data.csv'), index=False)

    df.rename(columns={
        "temperature_2m_min": "tempmin",
        "temperature_2m_mean": "temp",
        "temperature_2m_max": "tempmax",
        "time": "datetime"
    }, inplace=True)

    df[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(os.path.join(folder_path, 'temps.csv'), index=False)
    df[['datetime', 'weathercode']].to_csv(os.path.join(folder_path, 'conditions.csv'), index=False)

with DAG(
    "climate_data",
    start_date=pendulum.datetime(2025, 6, 20, tz="UTC"),
    schedule='0 0 * * 1',  
    catchup=True
) as dag:

    task1 = BashOperator(
        task_id='create_folder',
        bash_command='mkdir -p "/home/airflow/host_documents/climate_data_project/week{{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    task2 = PythonOperator(
    task_id="data_extract",
    python_callable=data_extract
)

    task1 >> task2

