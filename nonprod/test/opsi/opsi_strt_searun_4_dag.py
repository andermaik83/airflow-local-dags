"""
TEST OPSi Start SEArun 4 (tcOPSi_Strt_SEArun_4) migrated to Airflow.
- Schedule: minutes 00,20,40 between 05:50–23:45, every day
- Machine: tgen-vl105 (via resolve_connection_id)
- Command: /TEST/LIB/OPSi/proc/OPSi_start_SearchRun.sh 4
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
from airflow.timetables.trigger import MultipleCronTriggerTimetable

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

def _schedule_for_env(env_name: str):
    tz = "Europe/Brussels"
    if env_name == "TEST":
        #start_mins: 00,20,40
        #run_window: "05:50-23:45"
        return MultipleCronTriggerTimetable(
            "00,20,40 6-23 * * *",            
            timezone=tz,
        )
    elif env_name == "ACPT":
        #start_mins: 8,16,24,32,40,48,56
        #run_window: "06:20-04:50"
        return MultipleCronTriggerTimetable(
            "8,16,24,32,40,48,56 0-3 * * *",
            "8,16,24,32,40,48 4 * * *",
            "24,32,40,48 6 * * *",
            "8,16,24,32,40,48,56 7-23 * * *",            
            timezone=tz,
        )         
    elif env_name == "PROD":
        #start_mins: 00,11,22,33,44,55
        #run_window: "02:12-23:50"
        return MultipleCronTriggerTimetable(
            "22,33,44,55 2 * * *",
            "00,11,22,33,44,55 3-22 * * *",
            "00,11,22,33,44 23 * * *",
            timezone=tz,
        )

with DAG(
    dag_id=f"{env_pre}d_opsi_strt_searun_4",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} OPSi Start SEArun 4",
    schedule=_schedule_for_env(ENV),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'opsi', 'searun', 'start-4'],
):
    run_searun_4 = SSHOperator(
        task_id=f"{env_pre}cOPSi_Strt_SEArun_4",
        ssh_conn_id=SSH_CONN_ID,
        command=f"/{ENV}/LIB/OPSi/proc/OPSi_start_SearchRun.sh 4",
        doc_md=f"stderr: {STD_ERR_FILE}",
    )
