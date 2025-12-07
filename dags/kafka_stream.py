from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import random
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


# ---------------------------
# 生产者：写数据到 Kafka
# ---------------------------
def stream_to_kafka():
    producer = KafkaProducer(bootstrap_servers="broker:9092", max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # 跑 1 分钟
            break
        try:
            res = requests.get("https://randomuser.me/api/").json()["results"][0]

            location = res["location"]
            data = {
                "first_name": res["name"]["first"],
                "last_name": res["name"]["last"],
                "gender": res["gender"],
                "address": f"{location['street']['number']} {location['street']['name']}, "
                           f"{location['city']}, {location['state']}, {location['country']}",
                "post_code": location["postcode"],
                "email": res["email"],
                "username": res["login"]["username"],
                "dob": res["dob"]["date"],
                "registered_date": res["registered"]["date"],
                "phone": res["phone"],
                "picture": res["picture"]["medium"],
            }

            producer.send("users_created", json.dumps(data).encode("utf-8"))
            logging.info(f"Produced: {data['username']}")
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            logging.error(f"Producer error: {e}")
            continue


# ---------------------------
# 消费者：Kafka → Cassandra
# ---------------------------
def kafka_to_cassandra():
    # 连接 Cassandra
    cluster = Cluster(["cassandra"], port=9042)
    session = cluster.connect()

    # 确保 keyspace 和表存在
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streaming
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("spark_streaming")

    session.execute("""
        CREATE TABLE IF NOT EXISTS created_users (
            username TEXT PRIMARY KEY,
            address TEXT,
            dob TEXT,
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            phone TEXT,
            picture TEXT,
            registered_date TEXT
        )
    """)

    insert_stmt = session.prepare("""
        INSERT INTO created_users (username, address, dob, email, first_name, last_name, gender, phone, picture, registered_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    # 连接 Kafka
    consumer = KafkaConsumer(
        "users_created",
        bootstrap_servers="broker:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="user-group",
    )

    start_time = time.time()
    for msg in consumer:
        try:
            data = json.loads(msg.value.decode("utf-8"))
            session.execute(insert_stmt, (
                data["username"],
                data["address"],
                data["dob"],
                data["email"],
                data["first_name"],
                data["last_name"],
                data["gender"],
                data["phone"],
                data["picture"],
                data["registered_date"],
            ))
            logging.info(f"Inserted into Cassandra: {data['username']}")

        except Exception as e:
            logging.error(f"Consumer error: {e}")

        if time.time() > start_time + 90:  # 跑 1.5 分钟
            break


# ---------------------------
# Airflow DAG
# ---------------------------
default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "start_date": datetime(2023, 9, 3),
}

with DAG(
    dag_id="kafka_cassandra_pipeline",
    default_args=default_args,
    schedule_interval="@once",  # 手动触发即可
    catchup=False,
) as dag:

    produce_task = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=stream_to_kafka,
    )

    consume_task = PythonOperator(
        task_id="consume_to_cassandra",
        python_callable=kafka_to_cassandra,
    )

    produce_task >> consume_task