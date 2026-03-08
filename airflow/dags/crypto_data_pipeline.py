# type: ignore
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
     "email": ["datawhiz1akb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False
}

with DAG(
    dag_id="crypto_market_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    ingest_api = BashOperator(
        task_id="ingest_crypto_api",
        bash_command="set -a && source /opt/airflow/.env && set +a && python /opt/airflow/etl/extract/crypto_market_ingest.py"
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_transform",
        bash_command="set -a && source /opt/airflow/.env && set +a && python /opt/airflow/etl/transform/bronze_to_silver_crypto_market.py"
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_transform",
        bash_command="set -a && source /opt/airflow/.env && set +a && python /opt/airflow/etl/transform/silver_to_gold_crypto_market.py"
    )

    data_quality_check = BashOperator(
         task_id="data_quality_check",
        bash_command="set -a && source /opt/airflow/.env && set +a && python /opt/airflow/etl/data_quality/check_crypto_rowcount.py"
    )

    ingest_api >> bronze_to_silver >> silver_to_gold