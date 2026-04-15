from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.insert(0, "/Users/tunglam/Customer_Data_Pipeline/src")

# IMPORT FUNCTIONS RIÊNG (quan trọng)
from Pipeline import (
    extract,
    clean_data,
    add_customer_id,
    build_dim_customer,
    build_fact_customer_behavior,
    build_serving_customer_mart
)

from pyspark.sql import SparkSession

def create_spark():
    return SparkSession.builder.appName("Airflow-Pipeline").getOrCreate()


# ===== TASK FUNCTIONS =====

def task_extract():
    spark = create_spark()
    global df_raw
    df_raw = extract(spark, "data/raw.csv")

def task_clean():
    global df_clean
    df_clean = clean_data(df_raw)

def task_dim():
    global dim_customer
    df_base = add_customer_id(df_clean)
    dim_customer = build_dim_customer(df_base)

def task_fact():
    global fact
    df_base = add_customer_id(df_clean)
    fact = build_fact_customer_behavior(df_base)

def task_serving():
    build_serving_customer_mart(dim_customer, fact)


# ===== DAG =====

with DAG(
    dag_id="customer_pipeline_pro",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=task_extract)
    t2 = PythonOperator(task_id="clean", python_callable=task_clean)
    t3 = PythonOperator(task_id="build_dim", python_callable=task_dim)
    t4 = PythonOperator(task_id="build_fact", python_callable=task_fact)
    t5 = PythonOperator(task_id="build_serving", python_callable=task_serving)

    # dependency
    t1 >> t2 >> [t3, t4] >> t5