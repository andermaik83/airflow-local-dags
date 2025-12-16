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

# Utility import path for common utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from utils.common_utils import get_environment_from_path, resolve_connection_id

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
    schedule='*/20 * * * *',  # every 20 minutes
    catchup=False,
    max_active_runs=1,
    tags=[env, 'aruba', 'mail-client'],
)

aruba_mailclient_immediate = WinRMOperator(
        task_id=f"{env_pre}cARUBA_MailClient_Immediate",
        ssh_conn_id=WINDOWS_CONN_ID,
        command=r"E:\local\Aruba\mail-client\proc\mail-client-immediate.cmd",
        dag=dag,
        doc_md="""**tcARUBA_MailClient_Immediate** Windows CMD job""",
    )
