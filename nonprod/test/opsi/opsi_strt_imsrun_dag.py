"""
TEST OPSi Start IMS Run (tcOPSi_Strt_ImsRun) and Move IMS file (tcOPSi_Mv_Input_ImsRun) migrated to a single Airflow DAG.
- Schedule: Mo–Fr at minutes 25,50 between 06:15–23:15 (effective hours 06:25–22:50)
- Machine: tgen-vl105 (via resolve_connection_id)
- Commands:
  - /TEST/LIB/OPSi/proc/OPSi_start_IMSrun.sh Y
  - /TEST/LIB/OPSi/proc/OPSi_Move_IMSFilerun.sh RUN9 (runs after IMS run success)
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.timetables.trigger import CronTriggerTimetable, MultipleCronTriggerTimetable

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

def _schedule_for_env(env_name: str):
    tz = "Europe/Brussels"
    if env_name == "TEST":
        # Mon–Fri at :25,:50; window 06:15–23:15 => effective 06:25–22:50
        return CronTriggerTimetable("25,50 6-22 * * 1-5", timezone=tz)
    if env_name == "ACPT":
        # Mon–Sat at specific times
        return MultipleCronTriggerTimetable(
            "45 7 * * 1-6",
            "5 9 * * 1-6",
            "45 9 * * 1-6",
            "15 10 * * 1-6",
            "15 11 * * 1-6",
            "15 12 * * 1-6",
            "15 13 * * 1-6",
            "15 14 * * 1-6",
            "15 15 * * 1-6",
            "15 16 * * 1-6",
            "15 18 * * 1-6",
            "15 19 * * 1-6",
            timezone=tz,
        )
    if env_name == "PROD":
        # Daily minutes 12,32,53 within 02:23–23:50
        return MultipleCronTriggerTimetable(
            "32,53 2 * * *",                 # 02:32, 02:53 (exclude 02:12 < 02:23)
            "12,32,53 3-22 * * *",           # 03–22 at :12,:32,:53
            "12,32 23 * * *",                # 23:12, 23:32 (exclude 23:53 > 23:50)
            timezone=tz,
        )
    # Fallback (same as TEST)
    return CronTriggerTimetable("25,50 6-22 * * 1-5", timezone=tz)

# Window 06:15–23:15 with minutes 25,50 -> practical hours 6–22 at :25,:50
with DAG(
    dag_id=f"{env_pre}d_opsi_strt_imsrun",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} OPSi Start IMS run and move file",
    schedule=_schedule_for_env(ENV),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'opsi', 'ims'],
):
    start_ims = SSHOperator(
        task_id=f"{env_pre}cOPSi_Strt_ImsRun",
        ssh_conn_id=SSH_CONN_ID,
        command=f"/{ENV}/LIB/OPSi/proc/OPSi_start_IMSrun.sh Y",
    )

    move_ims = SSHOperator(
        task_id=f"{env_pre}cOPSi_Mv_Input_ImsRun",
        ssh_conn_id=SSH_CONN_ID,
        command=f"/{ENV}/LIB/OPSi/proc/OPSi_Move_IMSFilerun.sh RUN9",
    )

    start_ims >> move_ims
