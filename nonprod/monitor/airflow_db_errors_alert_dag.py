"""
DAG: airflow_db_errors_alert
Hourly monitoring DAG that checks for failed tasks/DAGs using Airflow ORM and sends a consolidated email.
Runs every hour and only sends email if failures are detected.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable, TaskInstance, DagRun
from airflow.utils.state import State
from airflow.utils.email import send_email
from airflow.utils.session import provide_session
from airflow.timetables.trigger import CronTriggerTimetable

ENV = Variable.get("ENVIRONMENT", default_var="nonprod")
MAIL_FROM = Variable.get("ALERT_MAIL_FROM", default_var="airflow-compumark@clarivate.com")
MAIL_TO = Variable.get("ALERT_MAIL_TO", default_var="ander.lopetegui@clarivate.com").split(",")
RECIPIENTS = Variable.get("alert_email_recipients", default_var="ander.lopetegui@clarivate.com").split(",")
LOOKBACK_HOURS = int(Variable.get("AIRFLOW_ALERT_LOOKBACK_HOURS", default_var=1))


TZ = "Europe/Brussels"

def _html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

@provide_session
def check_failures(session=None, **context):
    since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    failed_tasks = (
        session.query(TaskInstance)
        .filter(TaskInstance.state == State.FAILED)
        .filter(TaskInstance.end_date >= since)
        .all()
    )
    failed_dags = (
        session.query(DagRun)
        .filter(DagRun.state == State.FAILED)
        .filter(DagRun.end_date >= since)
        .all()
    )
    if not failed_tasks and not failed_dags:
        print(f"No failures detected in the last {LOOKBACK_HOURS} hour(s).")
        return None

    body = [
        f"<h2>Airflow Failures Report - {ENV} Environment</h2>",
        f"<p><strong>Time Range:</strong> Last {LOOKBACK_HOURS} hour(s) ending {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>"
    ]
    if failed_tasks:
        body.append(f"<h3>Failed Tasks ({len(failed_tasks)}):</h3>")
        body.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>")
        body.append("<tr style='background-color: #f0f0f0;'><th>DAG ID</th><th>Task ID</th><th>Run ID</th><th>End Time</th><th>Try Number</th><th>Duration</th></tr>")
        for ti in failed_tasks:
            start = ti.start_date
            end = ti.end_date
            dur = None
            if start and end:
                try:
                    dur = f"{(end - start).total_seconds():.1f}s"
                except:
                    pass
            body.append(
                f"<tr>"
                f"<td><strong>{_html_escape(ti.dag_id)}</strong></td>"
                f"<td>{_html_escape(ti.task_id)}</td>"
                f"<td style='font-size: 0.85em;'>{_html_escape(ti.run_id)}</td>"
                f"<td>{end or 'N/A'}</td>"
                f"<td>{ti.try_number}</td>"
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
                f"<td><strong>{_html_escape(dr.dag_id)}</strong></td>"
                f"<td style='font-size: 0.85em;'>{_html_escape(dr.run_id)}</td>"
                f"<td>{dr.execution_date}</td>"
                f"<td>{dr.end_date}</td>"
                f"</tr>"
            )
        body.append("</table>")
    body.append("<br><p><em>This is an automated report from Airflow failure monitoring.</em></p>")
    count = len(failed_tasks) + len(failed_dags)
    subject = f"[{ENV}] Airflow Failures: {count} in last {LOOKBACK_HOURS}h"
    return {"count": count, "subject": subject, "html": "".join(body)}

def send_email_if_failures(**context):
    report = context['ti'].xcom_pull(task_ids='check_failures')
    if not report or report["count"] == 0:
        print("No failures to report, skipping email.")
        return
    subject = report["subject"]
    html = report["html"]
    print(f"Failures detected: {report['count']}, sending email to {MAIL_TO} and {RECIPIENTS}...")
    send_email(
        to=list(set(MAIL_TO + RECIPIENTS)),
        subject=subject,
        html_content=html,
        from_email=MAIL_FROM
    )
    print(f"Email sent via SMTP to {MAIL_TO} and {RECIPIENTS}")

with DAG(
    dag_id="airflow_db_errors_alert",
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    description=f'{ENV} Airflow errors alert via mail relay (DB query)',
    schedule=CronTriggerTimetable("0 * * * *", timezone=TZ),  # every hour
    catchup=False,
    max_active_runs=1,
    tags=[ENV, 'monitor', 'alerts'],
) as dag:
    check = PythonOperator(
        task_id='check_failures',
        python_callable=check_failures,
    )
    notify = PythonOperator(
        task_id='send_email',
        python_callable=send_email_if_failures,
    )
    check >> notify
