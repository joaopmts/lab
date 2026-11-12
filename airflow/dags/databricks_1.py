"""Dispara jobs existentes no Databricks (Azure): extração CoinGecko -> transformação.

ATENÇÃO: cada run executa jobs pagos no Databricks. catchup=False para não disparar
uma run por dia desde start_date ao despausar.
"""
from datetime import datetime

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import DAG

with DAG(
    dag_id="coingeko-airflow-databricks-azure",
    start_date=datetime(2025, 10, 23),
    schedule="0 9 * * *",
    catchup=False,
    tags=["databricks", "azure", "cloud"],
) as dag:

    extraindo_dados = DatabricksRunNowOperator(
        task_id="extraindo_dados",
        databricks_conn_id="databricks_default",
        job_id=54040929043857,
        notebook_params={
            "data_execucao": "{{ (dag_run.data_interval_start or dag_run.run_after).strftime('%Y-%m-%d') }}",
        },
    )

    transformando_dados = DatabricksRunNowOperator(
        task_id="transformando_dados",
        databricks_conn_id="databricks_default",
        job_id=731042027537961,
    )

    extraindo_dados >> transformando_dados
