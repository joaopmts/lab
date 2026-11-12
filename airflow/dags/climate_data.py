"""Coleta semanal de temperaturas (Open-Meteo archive API) para Boston -> CSVs locais."""
import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pendulum
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

BASE_DIR = "/opt/airflow/data/climate_data_project"
LAT, LON = 42.3601, -71.0589
API_URL = "https://archive-api.open-meteo.com/v1/archive"


def week_window(interval_end: date, today: date) -> tuple[date, date]:
    """Semana de 7 dias a partir do fim do intervalo; recua uma semana se ainda não terminou."""
    start = interval_end
    end = start + timedelta(days=6)
    if end >= today:
        start -= timedelta(days=7)
        end = start + timedelta(days=6)
    return start, end


def data_extract(dag_run, **_):
    # Runs manuais no Airflow 3 não têm data interval: usa run_after como referência
    interval_end = dag_run.data_interval_end or dag_run.run_after
    today = datetime.now(timezone.utc).date()
    start, end = week_window(interval_end.date(), today)
    start_str, end_str = start.isoformat(), end.isoformat()

    response = requests.get(
        API_URL,
        params={
            "latitude": LAT,
            "longitude": LON,
            "start_date": start_str,
            "end_date": end_str,
            "daily": "temperature_2m_min,temperature_2m_mean,temperature_2m_max,weathercode",
            "timezone": "America/New_York",
        },
        timeout=30,
    )
    response.raise_for_status()
    df = pd.DataFrame(response.json()["daily"])

    folder_path = os.path.join(BASE_DIR, f"week{start_str}")
    os.makedirs(folder_path, exist_ok=True)
    df.to_csv(os.path.join(folder_path, "raw_data.csv"), index=False)

    df = df.rename(columns={
        "temperature_2m_min": "tempmin",
        "temperature_2m_mean": "temp",
        "temperature_2m_max": "tempmax",
        "time": "datetime",
    })
    df[["datetime", "tempmin", "temp", "tempmax"]].to_csv(os.path.join(folder_path, "temps.csv"), index=False)
    df[["datetime", "weathercode"]].to_csv(os.path.join(folder_path, "conditions.csv"), index=False)
    print(f"Dados de {start_str} a {end_str} salvos em {folder_path}")


with DAG(
    dag_id="climate_data",
    start_date=pendulum.datetime(2025, 6, 20, tz="UTC"),
    schedule="0 0 * * 1",
    # catchup desligado: ligar geraria uma run por semana desde start_date.
    # Para reprocessar um período use: airflow dags backfill climate_data -s ... -e ...
    catchup=False,
    tags=["api", "local-files"],
) as dag:

    PythonOperator(
        task_id="data_extract",
        python_callable=data_extract,
    )
