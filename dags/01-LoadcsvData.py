from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from psycopg2.extras import execute_values

base_dir = os.path.dirname(__file__)
data_dir = os.path.join(base_dir, 'data')
data_path = os.path.join(data_dir, "channels_stripped.csv")


def load_csv_to_postgres():
    pg = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg.get_conn()
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS channels(
                        id SERIAL PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        total_video_visit BIGINT CHECK(total_video_visit >= 0),
                        video_count INT CHECK(video_count >= 0),
                        start_date_timestamp BIGINT CHECK(start_date_timestamp >= 0),
                        followers_count BIGINT CHECK(followers_count >= 0),
                        country TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        update_count INT
                      );''')
    conn.commit()

    chunksize = 100000
    total_rows = 0

    for chunk_idx, chunk in enumerate(pd.read_csv(data_path, chunksize=chunksize), start=1):
        chunk.columns = chunk.columns.str.strip()

        records = list(chunk[['username', 'total_video_visit', 'video_count', 'start_date_timestamp',
                              'followers_count', 'country', 'update_count']].itertuples(index=False, name=None))

        sql = '''
            INSERT INTO channels(username, total_video_visit, video_count, start_date_timestamp,
                                  followers_count, country, update_count)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                username = EXCLUDED.username,
                total_video_visit = EXCLUDED.total_video_visit,
                video_count = EXCLUDED.video_count,
                start_date_timestamp = EXCLUDED.start_date_timestamp,
                followers_count = EXCLUDED.followers_count,
                country = EXCLUDED.country,
                update_count = EXCLUDED.update_count;
        '''

        execute_values(cursor, sql, records)
        conn.commit()

        total_rows += len(records)
        print(f"Chunk {chunk_idx}: Inserted/Updated {len(records)} rows. Total rows so far: {total_rows}")

    cursor.close()
    conn.close()


with DAG(
        dag_id='load_csv_to_postgreSQL',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        schedule_interval=None,
        tags=['csv', 'youtube']
) as dage:
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_csv_to_postgres
    )
