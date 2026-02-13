"""
Hourly monitoring DAG that checks for failed tasks/DAGs and sends a consolidated email.
Runs every hour and only sends email if failures are detected.
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.models import Variable
from airflow.utils.email import send_email

# Utility import path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from utils.common_utils import get_environment_from_path

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]

# Email config (override via Airflow Variables)
MAIL_FROM = Variable.get("ALERT_MAIL_FROM", default_var="airflow-compumark@clarivate.com")
MAIL_TO = Variable.get("ALERT_MAIL_TO", default_var="ander.lopetegui@clarivate.com").split(",")
LOOKBACK_HOURS = int(Variable.get("AIRFLOW_ALERT_LOOKBACK_HOURS", default_var=1))

TZ = "Europe/Brussels"
# Use Kubernetes service name when running in-cluster
WEBSERVER_URL = os.getenv("AIRFLOW__WEBSERVER__BASE_URL", "http://airflow-api-server.airflow-local.svc.cluster.local:8080")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def _html_escape(s: str) -> str:
    """Escape HTML special chars."""
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def check_failures(**context):
    """Query Airflow REST API for failed tasks/DAGs in the lookback window."""
    import requests
    from urllib.parse import urljoin
    
    cutoff = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    cutoff_iso = cutoff.isoformat() + "Z"
    
    failed_tasks = []
    failed_dags = []
    
    try:
        # Query failed task instances
        ti_url = urljoin(WEBSERVER_URL, "/api/v1/dags/~/dagRuns/~/taskInstances")
        ti_response = requests.get(
            ti_url,
            params={
                "state": "failed",
                "end_date_gte": cutoff_iso,
                "limit": 100,
                "order_by": "-end_date"
            },
            timeout=30
        )
        
        if ti_response.ok:
            data = ti_response.json()
            failed_tasks = data.get("task_instances", [])
            print(f"Found {len(failed_tasks)} failed task instances")
        else:
            print(f"Task instances API returned {ti_response.status_code}: {ti_response.text}")
        
        # Query failed DAG runs
        dr_url = urljoin(WEBSERVER_URL, "/api/v1/dags/~/dagRuns")
        dr_response = requests.get(
            dr_url,
            params={
                "state": "failed",
                "end_date_gte": cutoff_iso,
                "limit": 100,
                "order_by": "-end_date"
            },
            timeout=30
        )
        
        if dr_response.ok:
            data = dr_response.json()
            failed_dags = data.get("dag_runs", [])
            print(f"Found {len(failed_dags)} failed DAG runs")
        else:
            print(f"DAG runs API returned {dr_response.status_code}: {dr_response.text}")
            
    except Exception as e:
        print(f"Error querying Airflow API: {e}")
        import traceback
        traceback.print_exc()
    
    if not failed_tasks and not failed_dags:
        print(f"No failures detected in the last {LOOKBACK_HOURS} hour(s).")
        return None
    
    # Build HTML email body
    body = [
        f"<h2>Airflow Failures Report - {ENV} Environment</h2>",
        f"<p><strong>Time Range:</strong> Last {LOOKBACK_HOURS} hour(s) ending {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>"
    ]
    
    if failed_tasks:
        body.append(f"<h3>Failed Tasks ({len(failed_tasks)}):</h3>")
        body.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>")
        body.append("<tr style='background-color: #f0f0f0;'><th>DAG ID</th><th>Task ID</th><th>Run ID</th><th>End Time</th><th>Try Number</th><th>Duration</th></tr>")
        for ti in failed_tasks:
            start = ti.get("start_date")
            end = ti.get("end_date")
            dur = None
            if start and end:
                try:
                    start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                    dur = f"{(end_dt - start_dt).total_seconds():.1f}s"
                except:
                    pass
            
            body.append(
                f"<tr>"
                f"<td><strong>{_html_escape(ti.get('dag_id', 'N/A'))}</strong></td>"
                f"<td>{_html_escape(ti.get('task_id', 'N/A'))}</td>"
                f"<td style='font-size: 0.85em;'>{_html_escape(ti.get('dag_run_id', 'N/A'))}</td>"
                f"<td>{end or 'N/A'}</td>"
                f"<td>{ti.get('try_number', 'N/A')}</td>"
                f"<td>{dur or 'N/A'}</td>"
                f"</tr>"
            )
        body.append("</table>")
    
    if failed_dags:
        body.append(f"<h3>Failed DAG Runs ({len(failed_dags)}):</h3>")
        body.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>")
        body.append("<tr style='background-color: #f0f0f0;'><th>DAG ID</th><th>Run ID</th><th>Execution Date</th><th>End Time</th></tr>")
        for dr in failed_dags:
            body.append(
                f"<tr>"
                f"<td><strong>{_html_escape(dr.get('dag_id', 'N/A'))}</strong></td>"
                f"<td style='font-size: 0.85em;'>{_html_escape(dr.get('dag_run_id', 'N/A'))}</td>"
                f"<td>{dr.get('execution_date', 'N/A')}</td>"
                f"<td>{dr.get('end_date', 'N/A')}</td>"
                f"</tr>"
            )
        body.append("</table>")
    
    body.append("<br><p><em>This is an automated report from Airflow failure monitoring.</em></p>")
    
    count = len(failed_tasks) + len(failed_dags)
    subject = f"[{ENV}] Airflow Failures: {count} in last {LOOKBACK_HOURS}h"
    
    return {"count": count, "subject": subject, "html": "".join(body)}

def send_email_if_failures(**context):
    """Send email via SMTP if failures detected."""
    report = context['ti'].xcom_pull(task_ids=f'{env_pre}g_check_failures')
    
    if not report or report["count"] == 0:
        print("No failures to report, skipping email.")
        return
    
    subject = report["subject"]
    html = report["html"]
    
    print(f"Failures detected: {report['count']}, sending email to {MAIL_TO}...")
    
    # Use Airflow's send_email utility (uses SMTP config from airflow.cfg)
    send_email(
        to=MAIL_TO,
        subject=subject,
        html_content=html,
    )
    print(f"Email sent via SMTP to {MAIL_TO}")

with DAG(
    dag_id=f'{env_pre}d_airflow_errors_alert',
    default_args=DEFAULT_ARGS,
    description=f'{ENV} Airflow errors alert via mail relay',
    schedule=CronTriggerTimetable("0 * * * *", timezone=TZ),  # every hour
    catchup=False,
    max_active_runs=1,
    tags=[env, 'monitor', 'alerts'],
) as dag:
    
    check = PythonOperator(
        task_id=f'{env_pre}g_check_failures',
        python_callable=check_failures,
    )
    
    notify = PythonOperator(
        task_id=f'{env_pre}c_send_email',
        python_callable=send_email_if_failures,
    )
    
    check >> notify