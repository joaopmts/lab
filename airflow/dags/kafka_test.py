"""Teste de conectividade Airflow -> Kafka: produz 3 mensagens e consome de volta."""
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

KAFKA_BOOTSTRAP_SERVERS = "broker:19092"
TOPIC = "vendas"
MESSAGES = [
    '{"produto": "Notebook", "valor": 3500, "regiao": "SP"}',
    '{"produto": "Mouse", "valor": 120, "regiao": "RJ"}',
    '{"produto": "Monitor", "valor": 1500, "regiao": "MG"}',
]


def produce_message():
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    for i, msg in enumerate(MESSAGES, 1):
        producer.produce(TOPIC, key=str(i), value=msg)
        print(f"[Producer] Mensagem enviada: {msg}")
    producer.flush()


def consume_message(run_id, **_):
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # group novo por run: sempre relê o tópico desde o início
        "group.id": f"airflow-test-{run_id}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    msgs = []
    try:
        for _ in range(15):
            msg = consumer.poll(2.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Consumer] Erro: {msg.error()}")
                continue
            decoded = msg.value().decode("utf-8")
            print(f"[Consumer] Mensagem recebida: {decoded}")
            msgs.append(decoded)
            if len(msgs) >= len(MESSAGES):
                break
    finally:
        consumer.close()

    if not msgs:
        raise ValueError("Nenhuma mensagem recebida do Kafka!")


with DAG(
    dag_id="test_kafka_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "test"],
) as dag:

    produce_task = PythonOperator(task_id="produce_message", python_callable=produce_message)
    consume_task = PythonOperator(task_id="consume_message", python_callable=consume_message)

    produce_task >> consume_task
