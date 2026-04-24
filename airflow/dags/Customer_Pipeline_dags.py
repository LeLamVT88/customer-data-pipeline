from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="customer_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    BASE_CMD = "cd /opt/airflow/project && pip install pyspark && PYTHONPATH=/opt/airflow/project python3 -m"

    ingest = BashOperator(
        task_id="ingest_raw",
        bash_command=f"{BASE_CMD} src.jobs.ingest_raw"
    )

    clean = BashOperator(
        task_id="clean_data",
        bash_command=f"{BASE_CMD} src.jobs.clean_data"
    )

    dim_fact = BashOperator(
        task_id="build_dim_fact",
        bash_command=f"{BASE_CMD} src.jobs.build_dim_fact"
    )

    serving = BashOperator(
        task_id="build_serving",
        bash_command=f"{BASE_CMD} src.jobs.build_serving"
    )

    validate = BashOperator(
        task_id="validate_serving",
        bash_command=f"{BASE_CMD} src.jobs.validate_serving"
    )

    ingest >> clean >> dim_fact >> serving >> validate