"""
TEST WOWofl Offload Orders (tcWOWofl_OffloadOrders) migrated to Airflow.
- Schedule: Mon–Sat every 5 minutes
- Run window enforced via timetable: 04:00–02:45 (Europe/Brussels)
- Machine: tgen-vl106 (via resolve_connection_id)
- Command: /TEST/LIB/WOWofl/WOWofl_Offload/proc/WOWofl_OffloadOrders.sh
- stderr redirected to /TEST/SHR/WOWofl/log/WOWofl_OffloadOrders.log
- Mutual exclusion with cleanup approximated via pool 'wowofl_offload_mutex' (1 slot)
"""
from __future__ import annotations
from datetime import datetime, time, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable

# Utility import path for common utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from utils.common_utils import get_environment_from_path, resolve_connection_id

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]

SSH_CONN_ID = resolve_connection_id(ENV, 'opr_vl113')

DEFAULT_ARGS = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

CLEANUP_GATE_POOL = 'wowofl_cleanup_gate'
STD_ERR_FILE = f"/{ENV}/SHR/WOWofl/log/WOWofl_OffloadOrders.log"

# Window encoded directly in schedule via cron parts

with DAG(
    dag_id=f"{env_pre}d_wowofl_offload_orders",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} WOWofl Offload Orders",
    schedule=MultipleCronTriggerTimetable(
        '*/5 * * 1-6',                              # same-day ticks Mon–Sat
        '*/5 0-1 * * 2-6',                          # next-day 00–01 Tue–Sat
        '0,5,10,15,20,25,30,35,40 2 * * 2-6',       # next-day hour 02 capped at :45 Tue–Sat
        timezone='Europe/Brussels',
    ),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'wowofl', 'offload', 'orders'],
) as dag:

    run_orders = SSHOperator(
        task_id=f"{env_pre}cWOWofl_OffloadOrders",
        ssh_conn_id=SSH_CONN_ID,
        command=f"/{ENV}/LIB/WOWofl/WOWofl_Offload/proc/WOWofl_OffloadOrders.sh 2> {STD_ERR_FILE}",
        pool=CLEANUP_GATE_POOL,  # allow parallel with Orders; block on Cleanup
        pool_slots=1,
        doc_md=f"stderr: {STD_ERR_FILE}",
    )


