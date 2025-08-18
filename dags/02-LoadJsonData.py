from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
import json
from pymongo import MongoClient
from pymongo import InsertOne
import os

base_dir = os.path.dirname(__file__)
data_dir = os.path.join(base_dir, 'data')
data_path = os.path.join(data_dir, 'rep_videos.json')


def load_json_to_mongodb():
    logger = LoggingMixin().log

    logger.info("Connecting to MongoDB...")
    client = MongoClient("mongodb://mongo:27017/")
    database = client['youtube']
    collection = database['videos']

    logger.info("Reading JSON file (NDJSON) in stream mode...")

    batch_size = 100000
    batch = []
    total_inserted = 0

    try:
        with open(data_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    doc = json.loads(line)
                    batch.append(InsertOne(doc))
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON line skipped: {line[:100]}...")
                    continue

                if len(batch) >= batch_size:
                    try:
                        collection.bulk_write(batch, ordered=False)
                        total_inserted += len(batch)
                        logger.info(f"Inserted {total_inserted} records...")
                        batch.clear()
                    except Exception as e:
                        logger.error(f"Bulk write error: {e}")

            if batch:
                try:
                    collection.bulk_write(batch, ordered=False)
                    total_inserted += len(batch)
                except Exception as e:
                    logger.error(f"Bulk write error on final batch: {e}")

        logger.info(f"All {total_inserted} records inserted into MongoDB successfully.")
    except Exception as e:
        logger.error(f"Failed to process file {data_path}: {e}")
    finally:
        client.close()


with DAG(
    dag_id='load_json_to_mongoDB',
    start_date=datetime(2025,1,1),
    catchup=False,
    schedule_interval=None,
    tags=['json', 'youtube']
) as dag:
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_json_to_mongodb
    )




