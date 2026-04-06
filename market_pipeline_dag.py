from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from scripts.producer import fetch_and_stream
from scripts.processor import process_batch

with DAG('market_intelligence_pipeline', start_date=datetime(2024, 1, 1), schedule='@hourly') as dag:
    
    ingest_task = PythonOperator(
        task_id='ingest_from_api_to_kafka',
        python_callable=fetch_and_stream
    )

    process_task = PythonOperator(
        task_id='process_and_score_sentiment',
        python_callable=process_batch
    )

    ingest_task >> process_task