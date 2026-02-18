"""
Hourly monitoring DAG that checks for active DAGs whose last run failed and sends a consolidated email.
Runs every hour and only sends email if failures are detected.
Note: The report lists all active DAGs whose last run failed, regardless of when the last run occurred.
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import json

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
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

def _html_escape(s: str) -> str:
    """Escape HTML special chars."""
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def get_airflow_api_auth():
    """Retrieve Airflow API connection details from 'airflow_api' connection."""
    conn = BaseHook.get_connection('airflow_api')
    host = conn.host or WEBSERVER_URL
    login = conn.login or 'admin'
    password = conn.password or ''
    extra = conn.extra_dejson if hasattr(conn, 'extra_dejson') else {}
    return host, login, password, extra

def check_failures(**context):
    """Query Airflow REST API for active DAGs whose last run failed. Store result in XCom."""
    import requests
    from urllib.parse import urljoin, urlencode
    try:
        # Get API connection details
        api_host, airflow_user, airflow_pass, _ = get_airflow_api_auth()
        # Step 1: Get JWT token
        token_url = urljoin(api_host, "/auth/token")
        token_payload = {"username": airflow_user, "password": airflow_pass}
        token_headers = {"Content-Type": "application/json"}
        token_resp = requests.post(token_url, json=token_payload, headers=token_headers, timeout=10)
        if token_resp.status_code not in (200, 201):
            print(f"Failed to get JWT token: {token_resp.status_code} {token_resp.text}")
            return None
        token = token_resp.json().get("access_token") or token_resp.json().get("token")
        if not token:
            print(f"No access_token in response: {token_resp.text}")
            return None
        headers = {"Authorization": f"Bearer {token}"}

        # Step 2: Query DAGs whose last run failed, not paused, not stale
        params = {
            "last_dag_run_state": "failed",
            "exclude_stale": "true",
            "paused": "false",
            "limit": 1000
        }
        dags_url = urljoin(api_host, f"/api/v2/dags?{urlencode(params)}")
        dags_response = requests.get(dags_url, headers=headers, timeout=30)
        if not dags_response.ok:
            print(f"DAGs API returned {dags_response.status_code}: {dags_response.text}")
            return None
        print(f"DAGs API raw response: {dags_response.text}")
        dags_data = dags_response.json()
        failed_dags = []
        for dag in dags_data.get("dags", []):
            dag_id = dag.get("dag_id")
            last_parsed_time = dag.get("last_parsed_time")
            last_parse_duration = dag.get("last_parse_duration")
            if last_parsed_time:
                failed_dags.append({
                    "dag_id": dag_id,
                    "last_parsed_time": last_parsed_time,
                    "last_parse_duration": last_parse_duration
                })
        # Store failed_dags in XCom for change detection
        context['ti'].xcom_push(key='failed_dags_list', value=failed_dags)
        if not failed_dags:
            print("No active DAGs with last run failed.")
            return None
        # Build HTML email body
        body = [
            f"<h2>Airflow Failures Report - {ENV} Environment</h2>",
            f"<p><strong>Report Time:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>"
        ]
        body.append(f"<h3>Active DAGs with Last Run Failed ({len(failed_dags)}):</h3>")
        body.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>")
        body.append("<tr style='background-color: #f0f0f0;'><th>DAG ID</th><th>Last Parsed Time</th><th>Last Parse Duration (s)</th></tr>")
        for dr in failed_dags:
            body.append(
                f"<tr>"
                f"<td><strong>{_html_escape(dr.get('dag_id', 'N/A'))}</strong></td>"
                f"<td>{_html_escape(str(dr.get('last_parsed_time', 'N/A')))}</td>"
                f"<td>{dr.get('last_parse_duration', 'N/A')}</td>"
                f"</tr>"
            )
        body.append("</table>")
        body.append("<br><p><em>This is an automated report from Airflow failure monitoring.</em></p>")
        count = len(failed_dags)
        subject = f"[{ENV}] Airflow Failures: {count} active DAGs last run failed"
        return {"count": count, "subject": subject, "html": "".join(body), "failed_dags": failed_dags}
    except Exception as e:
        print(f"Error querying Airflow API: {e}")
        import traceback
        traceback.print_exc()
        return None

def send_email_if_failures(**context):
    """Send email via SMTP if failures detected and the list changed."""
    print("++++++++++++++++")
    print(f"mail to: {MAIL_TO}")
    print(f"mail from: {MAIL_FROM}")
    ti = context['ti']
    report = ti.xcom_pull(task_ids=f'{env_pre}g_check_failures')
    failed_dags = ti.xcom_pull(task_ids=f'{env_pre}g_check_failures', key='failed_dags_list')
    prev_failed_dags = ti.xcom_pull(task_ids=None, key='prev_failed_dags_list')
    # Compare current and previous failed_dags lists
    if failed_dags is not None and prev_failed_dags is not None:
        if json.dumps(failed_dags, sort_keys=True) == json.dumps(prev_failed_dags, sort_keys=True):
            print("No change in failed DAGs list, skipping email.")
            return
    # Store current failed_dags as previous for next run
    ti.xcom_push(key='prev_failed_dags_list', value=failed_dags)
    if not report or report.get("count", 0) == 0:
        print("No failures to report, skipping email.")
        return
    subject = report["subject"]
    html = report["html"]
    print(f"Failures detected: {report['count']}, sending email to {MAIL_TO}...")
    send_email(
        to=MAIL_TO,
        subject=subject,
        html_content=html
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