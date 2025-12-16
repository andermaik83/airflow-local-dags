"""
TEST Aruba MailClient Workflow DAG
Translated from CA Autosys tbARUBA_MailClient box:
- Runs every 5 minutes, all days
- Contains three Windows CMD jobs executed in parallel:
  * tcARUBA_MailClient_ForwardNewReport
  * tcARUBA_MailClient_Immediate
  * tcARUBA_MailClient_Update
alarm_if_fail: 0 (no alerts configured here)
"""
from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator

# Make shared utils importable when running under nonprod/test folder structure
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
try:
    from utils.common_utils import get_environment_from_path, resolve_connection_id
except Exception:
    # Fallbacks if utils can't be imported yet (e.g., during initial deploy)
    def get_environment_from_path(_file: str) -> str:
        return os.getenv('AIRFLOW_ENV', 'TEST').upper()
    def resolve_connection_id(env_name: str, _logical: str) -> str:
        # Default to TEST Windows monitor host
        return 'topr-vw103'

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]  # 't' for TEST, 'p' for PROD, etc.
app_name = 'aruba_mail_client'

# Resolve Windows connection id from logical name (maps to topr-vw103 in TEST)
WINDOWS_CONN_ID = resolve_connection_id(ENV, 'opr_vw104')

DEFAULT_ARGS = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id=f"{env_pre}d_{app_name}",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} Aruba MailClient Workflow",
    schedule='*/20 * * * *',  # every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=[env, 'aruba', 'mail-client'],
)

aruba_sync_pds_resources = SSHOperator(
    task_id=f"{env_pre}cARUBA_sync_pds_resources",
    ssh_conn_id=SSH_CONN_ID,
    command=f"/{ENV}/LIB/ARUBA/ARUBA_syncpdsresources/proc/ARUBA_syncpdsresources.sh  > {STDOUT_FILE} 2> {STDERR_FILE}",
    dag=dag,
    doc_md="""**pcARUBA_sync_pds_resources** Linux shell job executed daily at 04:00"""
)

# Box tbARUBA_MailClient represented as a TaskGroup; jobs run in parallel
with TaskGroup(group_id=f"{env_pre}bARUBA_MailClient", dag=dag) as aruba_mailclient_group:

    aruba_mailclientforward_new_report = WinRMOperator(
        task_id=f"{env_pre}cARUBA_MailClient_ForwardNewReport",
        ssh_conn_id=WINDOWS_CONN_ID,
        command=r"E:\local\Aruba\mail-client\proc\mail-client-forward-newReport.cmd",
        dag=dag,
        doc_md="""**tcARUBA_MailClient_ForwardNewReport** Windows CMD job""",
    )

    aruba_mailclient_immediate = WinRMOperator(
        task_id=f"{env_pre}cARUBA_MailClient_Immediate",
        ssh_conn_id=WINDOWS_CONN_ID,
        command=r"E:\local\Aruba\mail-client\proc\mail-client-immediate.cmd",
        dag=dag,
        doc_md="""**tcARUBA_MailClient_Immediate** Windows CMD job""",
    )

    aruba_mailclient_update = WinRMOperator(
        task_id=f"{env_pre}cARUBA_MailClient_Update",
        ssh_conn_id=WINDOWS_CONN_ID,
        command=r"E:\local\Aruba\mail-client\proc\mail-client-update.cmd",
        dag=dag,
        doc_md="""**tcARUBA_MailClient_Update** Windows CMD job""",
    )

    # No internal dependencies: jobs run in parallel per box semantics
    # forward_new_report, immediate, update

# Group is the entry point; no additional dependencies or completion marker
