from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

def say_hello():
    print("Hello from Airflow 3!")

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
):
    PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )
