from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "lakehouse"],
)
def hello_lakehouse():

    @task
    def hello():
        print("Hello from lakehouse Airflow!")

    @task
    def current_time():
        print(f"Current time: {datetime.now()}")

    hello() >> current_time()


hello_lakehouse()