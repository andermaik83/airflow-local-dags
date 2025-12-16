"""
TEST Aruba sync PDS resources DAG
Autosys CMD job tcARUBA_sync_pds_resources translated to Airflow.
- Runs daily at 04:00
- Executes a single Linux shell command via SSH.
"""
from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

# Ensure shared utils importable with current folder structure
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
try:
    from utils.common_utils import get_environment_from_path, resolve_connection_id
except Exception:
    def get_environment_from_path(_file: str) -> str:
        return os.getenv('AIRFLOW_ENV', 'TEST').upper()
    def resolve_connection_id(env_name: str, _logical: str) -> str:
        # Default TEST Linux host fallback
        return 'topr-vl103'

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]
app_name = 'aruba_sync_pds_resources'

# Linux host mapping (logical -> env-specific)
SSH_CONN_ID = resolve_connection_id(ENV, 'opr_vl111')

STDOUT_FILE = f"/{ENV}/SHR/ARUBA/log/sync-pds-resources-prod.stdout"
STDERR_FILE = f"/{ENV}/SHR/ARUBA/log/sync-pds-resources-prod.stderr"


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
    description=f"{ENV} Aruba sync PDS resources",
    schedule='0 4 * * *',  # daily at 04:00
    catchup=False,
    max_active_runs=1,
    tags=[env, 'aruba', 'sync-pds-resources'],
)

aruba_sync_pds_resources = SSHOperator(
    task_id=f"{env_pre}cARUBA_sync_pds_resources",
    ssh_conn_id=SSH_CONN_ID,
    command=f"/{ENV}/LIB/ARUBA/ARUBA_syncpdsresources/proc/ARUBA_syncpdsresources.sh  > {STDOUT_FILE} 2> {STDERR_FILE}",
    dag=dag,
    doc_md="""**pcARUBA_sync_pds_resources** Linux shell job executed daily at 04:00"""
)
