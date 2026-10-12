from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def hello_airflow():
    @task
    def extract():
        return ["Airflow", "Docker", "Windows"]

    @task
    def transform(items):
        return [item.upper() for item in items]

    @task
    def load(items):
        print(f"Processed items: {items}")

    load(transform(extract()))


hello_airflow()