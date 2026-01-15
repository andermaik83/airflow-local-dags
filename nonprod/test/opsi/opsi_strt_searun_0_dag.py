"""
TEST OPSi Start SEArun 0 (tcOPSi_Strt_SEArun_0) migrated to Airflow.
- Schedule: minutes 00,11,22,33,44,55 between 06:00–23:30, every day
- Machine: tgen-vl105 (via resolve_connection_id)
- Command: /TEST/LIB/OPSi/proc/OPSi_start_SearchRun.sh 0
"""
from __future__ import annotations
from datetime import datetime, timedelta
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

SSH_CONN_ID = resolve_connection_id(ENV, 'opr_vl111')

DEFAULT_ARGS = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

def _schedule_for_env(env_name: str):
    tz = "Europe/Brussels"
    if env_name == "TEST":
        return MultipleCronTriggerTimetable(
            "00,11,22,33,44,55 6-22 * * *",            
            "00,11,22 23 * * *",
            timezone=tz,
        )
    elif env_name == "ACPT":
        return MultipleCronTriggerTimetable(
            "15,30,45 0-4 * * *",
            "30,45 6 * * *",
            "15,30,45 7-23 * * *",            
            timezone=tz,
        )         
    elif env_name == "PROD":
        # Minutes 13,26,39,52 within 02:10–23:50 (cap 23:52), daily
        return MultipleCronTriggerTimetable(
            "13,26,39,52 2-22 * * *",
            "13,26,39 23 * * *",
            timezone=tz,
        )

with DAG(
    dag_id=f"{env_pre}d_opsi_strt_searun_0",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} OPSi Start SEArun 0",
    schedule=_schedule_for_env(ENV),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'opsi', 'searun', 'start-0'],
):
    run_searun_0 = SSHOperator(
        task_id=f"{env_pre}cOPSi_Strt_SEArun_0",
        ssh_conn_id=SSH_CONN_ID,
        command=f"/{ENV}/LIB/OPSi/proc/OPSi_start_SearchRun.sh 0",
    )
