from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def greet():
    print("Hello, Airflow!")

with DAG(
    dag_id='desafio',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['Alura'],
) as dag:
    
    greet_task = PythonOperator(
        task_id='greet_task',
        python_callable=greet,
    )
