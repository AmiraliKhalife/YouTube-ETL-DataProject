from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from pymongo import MongoClient


def load_json_to_mongo():
    # اتصال به MongoDB
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["train"]  # دیتابیس هدف
    collection = db["tst"]  # کالکشن هدف

    # خواندن فایل JSON
    with open("/opt/airflow/dags/data/sample.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # اگر JSON یک لیست از اسناد بود
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)


with DAG(
    dag_id="json_to_mongodb",
    start_date=datetime(2025, 8, 9),
    schedule_interval=None,  # فقط دستی اجرا میشه
    catchup=False
) as dag:

    task_load_json = PythonOperator(
        task_id="load_json_to_mongo",
        python_callable=load_json_to_mongo
    )

    task_load_json
