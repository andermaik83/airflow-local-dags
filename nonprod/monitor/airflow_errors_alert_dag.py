"""
Hourly monitoring DAG that checks for failed tasks/DAGs and sends a consolidated email.
Runs every hour and only sends email if failures are detected in the last hour.
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.session import create_session

# Utility import path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from utils.common_utils import get_environment_from_path

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,  # Don't email on monitor failures
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_failures(**context):
    """Query Airflow DB for failed tasks/DAGs in the last hour."""
    one_hour_ago = datetime.utcnow() - timedelta(hours=1)
    
    with create_session() as session:
        # Failed task instances in last hour
        failed_tasks = session.query(TaskInstance).filter(
            TaskInstance.state == State.FAILED,
            TaskInstance.end_date >= one_hour_ago
        ).all()
        
        # Failed DAG runs in last hour
        failed_dags = session.query(DagRun).filter(
            DagRun.state == State.FAILED,
            DagRun.end_date >= one_hour_ago
        ).all()
    
    if not failed_tasks and not failed_dags:
        print("No failures detected in the last hour.")
        return None
    
    # Build HTML email body
    body = [
        f"<h2>Airflow Failures Report - {ENV} Environment</h2>",
        f"<p><strong>Time Range:</strong> {one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>"
    ]
    
    if failed_tasks:
        body.append(f"<h3>Failed Tasks ({len(failed_tasks)}):</h3>")
        body.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>")
        body.append("<tr style='background-color: #f0f0f0;'><th>DAG ID</th><th>Task ID</th><th>Execution Date</th><th>End Time</th><th>Try Number</th></tr>")
        for ti in failed_tasks:
            body.append(
                f"<tr>"
                f"<td><strong>{ti.dag_id}</strong></td>"
                f"<td>{ti.task_id}</td>"
                f"<td>{ti.execution_date.strftime('%Y-%m-%d %H:%M:%S')}</td>"
                f"<td>{ti.end_date.strftime('%Y-%m-%d %H:%M:%S')}</td>"
                f"<td>{ti.try_number}</td>"
                f"</tr>"
            )
        body.append("</table>")
    
    if failed_dags:
        body.append(f"<h3>Failed DAG Runs ({len(failed_dags)}):</h3>")
        body.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>")
        body.append("<tr style='background-color: #f0f0f0;'><th>DAG ID</th><th>Execution Date</th><th>End Time</th><th>State</th></tr>")
        for dr in failed_dags:
            body.append(
                f"<tr>"
                f"<td><strong>{dr.dag_id}</strong></td>"
                f"<td>{dr.execution_date.strftime('%Y-%m-%d %H:%M:%S')}</td>"
                f"<td>{dr.end_date.strftime('%Y-%m-%d %H:%M:%S') if dr.end_date else 'N/A'}</td>"
                f"<td>{dr.state}</td>"
                f"</tr>"
            )
        body.append("</table>")
    
    body.append("<br><p><em>This is an automated report from Airflow failure monitoring.</em></p>")
    
    return "".join(body)

def send_email_if_failures(**context):
    """Send email only if there are failures."""
    failures = context['ti'].xcom_pull(task_ids='check_failures')
    
    if failures:
        print("Failures detected, sending email...")
        email_task = EmailOperator(
            task_id='send_email_dynamic',
            to=['ander.lopetegui@clarivate.com'],
            subject=f'Airflow Failures Detected - {ENV} Environment',
            html_content=failures,
        )
        email_task.execute(context)
        print("Email sent successfully.")
    else:
        print("No failures to report, skipping email.")

with DAG(
    dag_id=f'{env_pre}d_failure_monitor',
    default_args=DEFAULT_ARGS,
    description=f'{ENV} Hourly failure monitoring and email reporting',
    schedule='0 * * * *',  # Every hour at minute 0
    catchup=False,
    max_active_runs=1,
    tags=[env, 'monitoring', 'alerts', 'failures'],
) as dag:
    
    check = PythonOperator(
        task_id='check_failures',
        python_callable=check_failures,
    )
    
    notify = PythonOperator(
        task_id='send_email_if_failures',
        python_callable=send_email_if_failures,
    )
    
    check >> notify