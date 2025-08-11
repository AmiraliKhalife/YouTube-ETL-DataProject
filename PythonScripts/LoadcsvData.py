from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

base_dir = os.path.dirname(__file__)
data_dir = os.path.join(base_dir,'data')
data_path = os.path.join(data_dir, "channels_stripped.csv")

def load_csv_to_postgres():
    pg = PostgresHook(postgres_conn_id= 'postgres_conn')
    conn = pg.get_conn()
    cursor = conn.cursor()
    cursor.execute('''create table if not exists channels(
                                     id serial primary key,
                                     username text not null,
                                     total_video_visit bigint check(total_video_visit>=0),
                                     video_count int check(video_count>= 0),
                                     start_date_timestamp bigint check(start_date_timestamp>=0),
                                     followers_count bigint check(followers_count>=0),
                                     country text,
                                     created_at timestamp default current_timestamp,
                                     update_count int
                                     );''')
    
    chunksize = 50000  # Set this number based on your system's RAM and CPU.

    for chunk in pd.read_csv(data_path, chunksize=chunksize):
        chunk.columns = chunk.columns.str.strip()
        for _, row in chunk.iterrows():
            cursor.execute(
                '''insert into channels( username, total_video_visit, video_count, start_date_timestamp,
                                          followers_count, country, update_count)
                   values(%s, %s, %s, %s, %s, %s, %s)
                   on conflict(id) do update
                   set username= excluded.username,
                       total_video_visit= excluded.total_video_visit,
                       video_count= excluded.video_count,
                       start_date_timestamp= excluded.start_date_timestamp,
                       followers_count= excluded.followers_count,
                       country= excluded.country,
                       update_count= excluded.update_count;''',
                (row['username'], row['total_video_visit'], row['video_count'],
                 row['start_date_timestamp'], row['followers_count'],
                 row['country'], row['update_count'])
            )
        conn.commit()

    conn.close()

with DAG(
    dag_id='load_csv',
    start_date= datetime(2025,1,1),
    catchup= False,
    schedule_interval= None,
    tags=['csv,youtube']
) as dage:

    load_task=PythonOperator(
        task_id= 'load_task',
        python_callable= load_csv_to_postgres
    )

