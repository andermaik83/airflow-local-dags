"""
TEST WOWofl Offload Members (tcWOWofl_OffloadMembers) migrated to Airflow.
- Schedule: Mon–Sat at :06,:21,:36,:51 (6,21,36,51)
- Run window enforced via schedule (MultipleCronTriggerTimetable): 04:00–02:45 (Europe/Brussels)
- Command: /TEST/LIB/WOWofl/WOWofl_Offload/proc/WOWofl_OffloadNewMember.sh (per JIL)
- stderr redirected to /TEST/SHR/WOWofl/log/WOWofl_OffloadOrders.log (per JIL)
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
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

MUTEX_POOL = 'wowofl_offload_mutex'
STD_ERR_FILE = f"/{ENV}/SHR/WOWofl/log/WOWofl_OffloadOrders.log"

# Window encoded directly in schedule via two cron parts (Mon–Sat same-day, Tue–Sat early morning)

with DAG(
    dag_id=f"{env_pre}d_wowofl_offload_new_member",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} WOWofl Offload New Members",
    schedule=MultipleCronTriggerTimetable(
        '5,20,35,50 4-23 * * 1-6',  # same-day ticks 04:00–23:59 Mon–Sat
        '5,20,35,50 1 * * 2-6',     # next-day 00–01 Tue–Sat
        '5,20,35 2 * * 2-6',        # next-day hour 02 capped at :45 Tue–Sat
        timezone='Europe/Brussels',
    ),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'wowofl', 'offload', 'new_member'],
) as dag:

    run_members = SSHOperator(
        task_id=f"{env_pre}cWOWofl_OffloadNewMember",
        ssh_conn_id=SSH_CONN_ID,
        command=f"/{ENV}/LIB/WOWofl/WOWofl_Offload/proc/WOWofl_OffloadNewMember.sh 2> {STD_ERR_FILE}",
        pool=MUTEX_POOL,
        pool_slots=1,
        doc_md=f"stderr: {STD_ERR_FILE}",
    )


