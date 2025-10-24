from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id='alura_databricks_1',
    start_date=datetime(2025, 8, 23),
    schedule='0 9 * * *',
    catchup=True,
) as dag_executando_notebook_extracao:

    extraindo_dados = DatabricksRunNowOperator(
        task_id="Extraindo-conversoes",
        databricks_conn_id="databricks_default",
        job_id=54040929043857,
        notebook_params={
            "data_execucao": "{{ data_interval_start.strftime('%Y-%m-%d') }}"
        },
    )

    extraindo_dados
