from airflow.models import DAG
from airflow.utils.dates import days_ago

with DAG(
    'first_dag',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag: