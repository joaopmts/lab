from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from confluent_kafka import Producer, Consumer


KAFKA_BOOTSTRAP_SERVERS = "broker:19092" 
TOPIC = "vendas"


def produce_message():
    """Produz algumas mensagens para o Kafka"""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    messages = [
        '{"produto": "Notebook", "valor": 3500, "regiao": "SP"}',
        '{"produto": "Mouse", "valor": 120, "regiao": "RJ"}',
        '{"produto": "Monitor", "valor": 1500, "regiao": "MG"}',
    ]

    for i, msg in enumerate(messages, 1):
        producer.produce(TOPIC, key=str(i), value=msg)
        print(f"[Producer] Mensagem enviada: {msg}")

    producer.flush()


def consume_message():
    """Consome mensagens do Kafka"""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "airflow-test",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC])

    msgs = []
    for _ in range(5):
        msg = consumer.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[Consumer] Erro: {msg.error()}")
        else:
            decoded = msg.value().decode("utf-8")
            print(f"[Consumer] Mensagem recebida: {decoded}")
            msgs.append(decoded)

    consumer.close()

    if not msgs:
        raise ValueError("Nenhuma mensagem recebida do Kafka!")


default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="test_kafka_dag",
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=["kafka", "teste"],
) as dag:

    produce_task = PythonOperator(
        task_id="produce_message",
        python_callable=produce_message
    )

    consume_task = PythonOperator(
        task_id="consume_message",
        python_callable=consume_message
    )

    produce_task >> consume_task
