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
        bash_command=(
            "DATA_DATE={{ ds }} "
            "python /opt/airflow/scripts/ingestion/bybit_btc_klines.py"
        ),
    )

    load_to_iceberg = BashOperator(
        task_id="load_to_iceberg",
        bash_command=(
            "DATA_DATE={{ ds }} "
            "python /opt/airflow/scripts/iceberg/load_btc_klines_to_iceberg.py"
        ),
    )

    validate_iceberg_load = BashOperator(
        task_id="validate_iceberg_load",
        bash_command=(
            "DATA_DATE={{ ds }} "
            "python /opt/airflow/scripts/validation/validate_btc_klines_iceberg.py"
        ),
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
        cd /opt/airflow/dbt/local_lakehouse &&
        dbt run
        """,
    )

    spark_batch_ingest = BashOperator(
        task_id="spark_batch_ingest",
        bash_command=(
            "python /opt/airflow/spark_jobs/batch_ingest_crypto.py "
            "--date {{ ds }}"
        ),
    )


    ingest_btc_data >> spark_batch_ingest >> validate_iceberg_load >> run_dbt_models


btc_ingestion_pipeline()