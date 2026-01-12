"""
TEST OPSi SearchOrdersPerAnalyst (tcOPSi_SearchOrdersPerAnalyst) migrated to Airflow.
- Schedule: Mo–Fr at 06:00
- Machine: tgen-vl101 (via resolve_connection_id)
- Command: /TEST/LIB/OPSi/proc/OPSi_SearchOrdersPerAnalyst.sh
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.timetables.trigger import CronTriggerTimetable

# Utility import path for common utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from utils.common_utils import get_environment_from_path, resolve_connection_id

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]

SSH_CONN_ID = resolve_connection_id(ENV, 'opr_vl101')

DEFAULT_ARGS = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=f"{env_pre}d_opsi_search_orders_per_analyst",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} OPSi SearchOrdersPerAnalyst",
    schedule=CronTriggerTimetable(
        '0 5 * * 1-5',
        timezone='Europe/Brussels',
    ),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'opsi', 'search-orders-per-analyst'],
):
    search_orders = SSHOperator(
        task_id=f"{env_pre}cOPSi_SearchOrdersPerAnalyst",
        ssh_conn_id=SSH_CONN_ID,
        command=f"/{ENV}/LIB/OPSi/proc/OPSi_SearchOrdersPerAnalyst.sh ",
    )
