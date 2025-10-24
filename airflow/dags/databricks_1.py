from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id='coingeko-airflow-databricks-azure',
    start_date=datetime(2025, 10, 23),
    schedule='0 9 * * *',
    catchup=True,
) as dag_executando_notebook_extracao:

    extraindo_dados = DatabricksRunNowOperator(
        task_id="extraindo_dados",
        databricks_conn_id="databricks_default",
        job_id=54040929043857,
        notebook_params={
            "data_execucao": "{{ data_interval_start.strftime('%Y-%m-%d') }}"
        },
    )


    transformando_dados = DatabricksRunNowOperator(
        task_id="transformando_dados",
        databricks_conn_id="databricks_default",
        job_id=731042027537961,
    )


    extraindo_dados >> transformando_dados
