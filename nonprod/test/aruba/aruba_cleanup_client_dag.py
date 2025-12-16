"""
TEST Aruba Cleanup Client DAG
Autosys CMD job tcARUBA_Cleanupclient translated to Airflow.
- Runs every 10 minutes at minutes 05,15,25,35,45,55
- Executes a single Windows command via WinRM.
- No alerts (alarm_if_fail: 0)
"""
from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator

# Ensure shared utils importable with current folder structure
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
try:
    from utils.common_utils import get_environment_from_path, resolve_connection_id
except Exception:
    def get_environment_from_path(_file: str) -> str:
        return os.getenv('AIRFLOW_ENV', 'TEST').upper()
    def resolve_connection_id(env_name: str, _logical: str) -> str:
        return 'topr-vw103'

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]
app_name = 'aruba_cleanup_client'

# Windows host mapping (logical -> env-specific)
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
    description=f"{ENV} Aruba Cleanup Client",
    schedule='5/10 * * * *',  # minutes: 05,15,25,35,45,55
    catchup=False,
    max_active_runs=1,
    tags=[env, 'aruba', 'cleanup-client'],
)

cleanup_task = WinRMOperator(
    task_id=f"{env_pre}cARUBA_Cleanupclient",
    ssh_conn_id=WINDOWS_CONN_ID,
    command=r"E:\local\Aruba\cleanup-client\proc\cleanup-client.cmd",
    dag=dag,
    doc_md="""**tcARUBA_Cleanupclient** Windows CMD job executed every 10 minutes at :05 offsets.""",
)
