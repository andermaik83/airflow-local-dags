from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_hello():
    return 'Hello from local Airflow!'

default_args = {
    'owner': 'eva-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'eva_sample_dag',
    default_args=default_args,
    description='A sample DAG for EVA project local testing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['eva', 'sample'],
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

t3 = BashOperator(
    task_id='eva_cleanup_tmp',
    bash_command='echo "Simulating EMR tmp cleanup: rm -rf /tmp/*"',
    dag=dag,
)

t1 >> t2 >> t3
