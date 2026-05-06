import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

logger = logging.getLogger(__name__)

default_args = {
    "owner":            "data_engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="weather_etl_pipeline",
    description="Extract → Transform → Load weather data every 6 hours",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 */6 * * *",   # run every 6 hours
    catchup=False,
    tags=["etl", "weather", "postgres"],
)

def task_extract(**context):
    from extract import extract_all_cities
    raw_data = extract_all_cities()
    if not raw_data:
        raise ValueError("No data extracted from API")
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    logger.info(f"Extracted {len(raw_data)} cities")

def task_transform(**context):
    from transform import transform
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    df = transform(raw_data)
    if df.empty:
        raise ValueError("Transform produced empty DataFrame")
    context["ti"].xcom_push(key="clean_data", value=df.to_dict(orient="records"))
    logger.info(f"Transformed {len(df)} records")

def task_load(**context):
    import pandas as pd
    from load import load_raw, load_summary
    records  = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    df       = pd.DataFrame(records)
    rows     = load_raw(df)
    load_summary()
    logger.info(f"Loaded {rows} rows and refreshed summary")

extract_task = PythonOperator(
    task_id="extract", python_callable=task_extract, dag=dag
)
transform_task = PythonOperator(
    task_id="transform", python_callable=task_transform, dag=dag
)
load_task = PythonOperator(
    task_id="load", python_callable=task_load, dag=dag
)

extract_task >> transform_task >> load_task