from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import io
import csv
import logging


batch_size = 100000


def create_channels_table():

    query = '''CREATE TABLE IF NOT EXISTS bronze_channels
                 (
                     id UInt32,
                     username String,
                     total_video_visit UInt64,
                     video_count UInt32,
                     start_date_timestamp UInt64,
                     followers_count UInt64,
                     country String,
                     created_at DateTime64(6) DEFAULT now(),
                     update_count UInt32
                 )
                 ENGINE = MergeTree
                 ORDER BY (id)'''

    ch_hook = HttpHook(http_conn_id='ch_conn', method='POST')
    ch_hook.run(endpoint="/", data=query.encode("utf-8"))


def load_data_to_clickhouse():
    pg = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg.get_conn()
    cursor = conn.cursor()
    ch_hook = HttpHook(http_conn_id='ch_conn', method='POST')

    last_id = 0
    total_inserted = 0

    while True:
        cursor.execute(f"""
            SELECT id, username, total_video_visit, video_count, start_date_timestamp,
                   followers_count, country, created_at, update_count
            FROM channels
            WHERE id > {last_id}
            ORDER BY id
            LIMIT {batch_size};
        """)
        rows = cursor.fetchall()
        if not rows:
            break

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        for r in rows:
            writer.writerow(r)
        csv_data = output.getvalue()
        output.close()

        insert_query = """INSERT INTO bronze_channels
        (id, username, total_video_visit, video_count, start_date_timestamp,
         followers_count, country, created_at, update_count)
        FORMAT CSV
        """

        ch_hook.run(endpoint="/", data=(insert_query + csv_data).encode("utf-8"))

        last_id = rows[-1][0]
        total_inserted += len(rows)

        logging.info(f"So far {total_inserted:,} rows have been inserted into ClickHouse.")

    cursor.close()
    conn.close()
    logging.info(f"Load finished. Total rows inserted: {total_inserted:,}")


with DAG(
    dag_id='postgres_to_clickhouse',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None,
    tags=['youtube', 'clickHouse']
) as dag:

    create_table = PythonOperator(
        task_id='create_bronze_table',
        python_callable=create_channels_table
    )

    load_to_ch = PythonOperator(
        task_id='load_to_clickHouse',
        python_callable=load_data_to_clickhouse
    )

create_table >> load_to_ch
