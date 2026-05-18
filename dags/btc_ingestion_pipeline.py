from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(
    dag_id="btc_ingestion_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "btc", "iceberg"],
)
def btc_ingestion_pipeline():

    ingest_btc_data = BashOperator(
        task_id="ingest_btc_data",
        bash_command="python /opt/airflow/scripts/ingestion/bybit_btc_klines.py",
    )

    load_to_iceberg = BashOperator(
        task_id="load_to_iceberg",
        bash_command="python /opt/airflow/scripts/iceberg/load_btc_klines_to_iceberg.py",
    )

    ingest_btc_data >> load_to_iceberg


btc_ingestion_pipeline()