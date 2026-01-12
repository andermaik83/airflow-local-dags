"""
TEST OPSi WatchContactTrialPeriod (tcOPSi_WatchContactTrialPeriod) migrated to Airflow.
- Schedule: daily at 07:00
- Machine: topr-vw103 (via resolve_connection_id)
- Command: E:\local\opsi\proc\OPSi_WatchContactTrialPeriod.cmd
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from airflow.timetables.trigger import CronTriggerTimetable

# Utility import path for common utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from utils.common_utils import get_environment_from_path, resolve_connection_id

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]

WINDOWS_CONN_ID = resolve_connection_id(ENV, 'opr_vw104')

DEFAULT_ARGS = {
    'owner': 'test@INT',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id=f"{env_pre}d_opsi_watch_contact_trial_period",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} OPSi WatchContactTrialPeriod",
    schedule=CronTriggerTimetable(
        '0 7 * * *',
        timezone='Europe/Brussels',
    ),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'opsi', 'watch-contact-trial'],
):
    watch_contact_trial = WinRMOperator(
        task_id=f"{env_pre}cOPSi_WatchContactTrialPeriod",
        ssh_conn_id=WINDOWS_CONN_ID,
        command=r"E:\\local\\opsi\\proc\\OPSi_WatchContactTrialPeriod.cmd",
    )
