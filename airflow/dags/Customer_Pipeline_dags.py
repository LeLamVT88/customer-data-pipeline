from datetime import datetime
import sys
import os

PROJECT_ROOT = "/Users/tunglam/Customer_Data_Pipeline"
sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.jobs.ingest_raw import run as ingest_raw_run
from src.jobs.clean_data import run as clean_data_run
from src.jobs.build_dim_fact import run as build_dim_fact_run
from src.jobs.build_serving import run as build_serving_run
from src.jobs.validate_serving import run as validate_serving_run

from src.utils.config import (
    SOURCE_CSV_PATH,
    RAW_PATH,
    CLEAN_PATH,
    DIM_CUSTOMER_PATH,
    FACT_SPENDING_PATH,
    SERVING_PATH,
)

default_args = {
    "owner": "tunglam",
}

with DAG(
    dag_id="customer_data_pipeline_production",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["customer", "pyspark", "airflow"],
) as dag:

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw_run,
        op_kwargs={
            "source_csv_path": SOURCE_CSV_PATH,
            "raw_output_path": RAW_PATH,
        },
    )

    clean_data = PythonOperator(
        task_id="clean_customer_data",
        python_callable=clean_data_run,
        op_kwargs={
            "raw_input_path": RAW_PATH,
            "clean_output_path": CLEAN_PATH,
        },
    )

    build_dim_fact = PythonOperator(
        task_id="build_dim_fact",
        python_callable=build_dim_fact_run,
        op_kwargs={
            "clean_input_path": CLEAN_PATH,
            "dim_output_path": DIM_CUSTOMER_PATH,
            "fact_output_path": FACT_SPENDING_PATH,
        },
    )

    build_serving = PythonOperator(
        task_id="build_customer_mart",
        python_callable=build_serving_run,
        op_kwargs={
            "dim_path": DIM_CUSTOMER_PATH,
            "fact_path": FACT_SPENDING_PATH,
            "serving_output_path": SERVING_PATH,
        },
    )

    validate_serving = PythonOperator(
        task_id="validate_customer_mart",
        python_callable=validate_serving_run,
        op_kwargs={
            "serving_path": SERVING_PATH,
        },
    )

    ingest_raw >> clean_data >> build_dim_fact >> build_serving >> validate_serving