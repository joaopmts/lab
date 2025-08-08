from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator    

with DAG(
    'first_dag',
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:
    task1 = EmptyOperator(task_id='task1')
    task2 = EmptyOperator(task_id='task2')
    task3 = EmptyOperator(task_id='task3')
    task4 = BashOperator(
        task_id='create_folder',
        bash_command='mkdir -p /home/airflow/host_documents/pasta{{data_interval_end}}'
        )

    task1 >> [task2, task3]
    task3 >> task4