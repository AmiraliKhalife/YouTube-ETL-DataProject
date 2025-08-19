from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import io
import csv
import logging

batch_size = 100000


def create_videos_table():

    query = '''create table if not exists bronze_videos
    (
        id UInt32,
        owner_username String,
        owner_id String,
        title String,
        tags String,
        uid String,
        visit_count UInt64,
        owner_name String,
        duration UInt32,
        posted_date DateTime,
        posted_timestamp UInt64,
        comments String,
        like_count UInt64,
        description String,
        is_deleted UInt8,
        created_at DateTime64(6) DEFAULT now()
    )
    ENGINE = MergeTree
    order by (id)'''

    ch_hook = HttpHook(http_conn_id='ch_conn', method= "POST")
    ch_hook.run(endpoint="/", data=query.encode("utf-8"))


def load_json_to_clickhouse():
    client = MongoClient("mongodb://mongo:27017/")
    db = client["youtube"]
    collection = db["videos"]

    ch_hook = HttpHook(http_conn_id='ch_conn', method='POST')
    cursor = collection.find({}, no_cursor_timeout=True).sort('id', 1)

    total_inserted = 0
    buffer = []

    for doc in cursor:
        row = [
            int(doc.get("id", 0)),
            doc.get("owner_username", ""),
            doc.get("owner_id", ""),
            doc.get("title", ""),
            doc.get("tags", ""),
            doc.get("uid", ""),
            int(doc.get("visit_count", 0)),
            doc.get("owner_name", ""),
            int(doc.get("duration", 0)),
            doc.get("posted_date") or "\\N",
            int(doc.get("posted_timestamp", 0)),
            doc.get("comments", ""),
            int(doc.get("like_count", 0)),
            doc.get("description", ""),
            1 if doc.get("is_deleted", False) else 0
        ]
        buffer.append(row)

        if len(buffer) >= batch_size:
            output = io.StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
            for r in buffer:
                writer.writerow(r)
            csv_data = output.getvalue()
            output.close()

            insert_query = '''INSERT INTO bronze_videos
            (id, owner_username, owner_id, title, tags, uid, visit_count,
             owner_name, duration, posted_date, posted_timestamp,
             comments, like_count, description, is_deleted)
            FORMAT CSV'''

            ch_hook.run(
                endpoint="/",
                data=(insert_query + "\n" + csv_data).encode("utf-8"),
                headers={"Content-Type": "text/csv"}
            )

            total_inserted += len(buffer)
            logging.info(f"So far inserted {total_inserted} rows into ClickHouse.")
            buffer.clear()

    if buffer:
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        for r in buffer:
            writer.writerow(r)
        csv_data = output.getvalue()
        output.close()

        insert_query = '''INSERT INTO bronze_videos
        (id, owner_username, owner_id, title, tags, uid, visit_count,
         owner_name, duration, posted_date, posted_timestamp,
         comments, like_count, description, is_deleted)
        FORMAT CSV'''

        payload = insert_query + "\n" + csv_data
        ch_hook.run(endpoint="/", data=payload.encode("utf-8"), headers={"Content-Type": "text/csv"}
        )

        total_inserted += len(buffer)
        logging.info(f"Final insert: total {total_inserted} rows inserted into ClickHouse")

    cursor.close()
    client.close()


with DAG(
    dag_id="MongoDB_to_clickhouse",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None,
    tags=['youtube', 'clickhouse']
) as dag:

    create_table = PythonOperator(
        task_id='create_bronze_table',
        python_callable=create_videos_table
    )

    load_to_ch = PythonOperator(
        task_id='load_to_ClickHouse',
        python_callable=load_json_to_clickhouse
    )

    create_table >> load_to_ch
