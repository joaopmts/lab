"""Exemplo de integração entre todos os serviços do lab (manual).

  NiFi ──┐  (flow "lab_example": GenerateFlowFile -> PublishKafka + PutS3Object)
         ├──> Kafka: lab_events
Airflow ─┘  (produce_events)
                │
                ▼  spark_kafka_to_lakehouse (SparkSubmitOperator -> cluster Spark)
   Iceberg iceberg.lab.events  +  Delta lab.events_summary   (Hive Metastore + MinIO)
                │                          │
                │                          └──> Kafka: lab_events_summary
                ▼
   Trino: valida as tabelas, cria iceberg.lab.events_by_source (CTAS) e lê o Kafka
                │
                ▼
   consume_summary: Airflow lê o resumo de volta do Kafka
"""
import json
import random
import uuid
from datetime import datetime, timezone

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

BOOTSTRAP = "broker:19092"
EVENT_TYPES = ["click", "view", "purchase"]


def produce_events(n=20):
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for _ in range(n):
        event = {
            "event_id": str(uuid.uuid4()),
            "source": "airflow",
            "event_type": random.choice(EVENT_TYPES),
            "value": round(random.uniform(1, 1000), 2),
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        }
        producer.produce("lab_events", key=event["event_id"], value=json.dumps(event))
    producer.flush()
    print(f"{n} eventos enviados para lab_events")


def consume_summary(run_id, **_):
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"lab-summary-{run_id}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["lab_events_summary"])
    msgs = []
    try:
        for _ in range(15):
            msg = consumer.poll(2.0)
            if msg is not None and not msg.error():
                msgs.append(json.loads(msg.value()))
            elif msgs:
                break
    finally:
        consumer.close()
    if not msgs:
        raise ValueError("Nenhuma mensagem em lab_events_summary")
    for m in msgs[-6:]:
        print(m)


with DAG(
    dag_id="lab_end_to_end",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "integration", "kafka", "spark", "trino", "nifi"],
    doc_md=__doc__,
) as dag:

    produce = PythonOperator(task_id="produce_events", python_callable=produce_events)

    spark_job = SparkSubmitOperator(
        task_id="spark_kafka_to_lakehouse",
        application="/opt/airflow/dags/scripts/lab_kafka_to_lakehouse.py",
        name="lab_kafka_to_lakehouse",
        conn_id="spark_default",
    )

    trino_check = SQLCheckOperator(
        task_id="trino_check_tables",
        conn_id="trino_default",
        sql="""
            SELECT
              (SELECT count(*) FROM iceberg.lab.events) > 0,
              (SELECT count(*) FROM delta.lab.events_summary) > 0,
              (SELECT sum(n_events) FROM delta.lab.events_summary)
                = (SELECT count(*) FROM iceberg.lab.events)
        """,
    )

    trino_ctas = SQLExecuteQueryOperator(
        task_id="trino_create_events_by_source",
        conn_id="trino_default",
        sql=[
            "DROP TABLE IF EXISTS iceberg.lab.events_by_source",
            """CREATE TABLE iceberg.lab.events_by_source AS
               SELECT source, count(*) AS n_events, round(avg(value), 2) AS avg_value
               FROM iceberg.lab.events GROUP BY source""",
        ],
    )

    trino_kafka = SQLCheckOperator(
        task_id="trino_read_kafka",
        conn_id="trino_default",
        sql="SELECT count(*) > 0 FROM kafka.default.lab_events_summary",
    )

    consume = PythonOperator(task_id="consume_summary", python_callable=consume_summary)

    produce >> spark_job >> trino_check >> [trino_ctas, trino_kafka] >> consume
