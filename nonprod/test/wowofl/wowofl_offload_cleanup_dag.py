"""
TEST WOWofl Offload Cleanup (tcWOWofl_OffloadCleanup) migrated to Airflow.
- Schedule: Mon–Sat at 03:00
- Machine: tgen-vl105 (via resolve_connection_id)
- Command: /TEST/LIB/WOWofl/WOWofl_Offload/proc/WOWofl_OffloadCleanup.sh
- stderr redirected to /TEST/SHR/WOWofl/log/WOWofl_OffloadCleanup.log
- Mutual exclusion approximated via pool 'wowofl_offload_mutex' (1 slot)
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

# Utility import path for common utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from utils.common_utils import get_environment_from_path, resolve_connection_id

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]

SSH_CONN_ID = resolve_connection_id(ENV, 'opr_vl113')

STD_ERR_FILE = f"/{ENV}/SHR/WOWofl/log/WOWofl_OffloadCleanup.log"

DEFAULT_ARGS = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

MUTEX_POOL = 'wowofl_offload_mutex'

with DAG(
    dag_id=f"{env_pre}d_wowofl_offload_cleanup",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} WOWofl Offload Cleanup",
    schedule='0 3 * * 1-7',
    catchup=False,
    max_active_runs=1,
    dag=dag,
    tags=[env, 'wowofl', 'offload', 'cleanup'],
)
    
wowofl_offloadcleanup = SSHOperator(
    task_id=f"{env_pre}cWOWofl_OffloadCleanup",
    ssh_conn_id=SSH_CONN_ID,
    command=f"/{ENV}/LIB/WOWofl/WOWofl_Offload/proc/WOWofl_OffloadCleanup.sh 2> {STD_ERR_FILE}'",
    pool=MUTEX_POOL,
    pool_slots=1,
    doc_md=f"stderr: {STD_ERR_FILE}",
)

