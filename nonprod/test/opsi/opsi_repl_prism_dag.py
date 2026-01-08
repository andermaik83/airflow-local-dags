"""
TEST OPSi Replicate Prism (tcOPSi_Repl_Prism) migrated to Airflow.
- Schedule: Every day at minutes 9,19,29,39,49,59 (every 10 minutes)
- Machine: topr-vw103 (via resolve_connection_id)
- Command: E:\local\OPSi\proc\OPSi_Repl_Prism.cmd
- stderr redirected to /TEST/SHR/OPSi/log/OPSi_Repl_Prism.log
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

# Autosys machine: topr-vw103
SSH_CONN_ID = resolve_connection_id(ENV, 'opr_vw104')

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
    dag_id=f"{env_pre}d_opsi_repl_prism",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} OPSi Replicate Prism",
    schedule=CronTriggerTimetable(
        '9,19,29,39,49,59 * * * *',
        timezone='Europe/Brussels',
    ),
    catchup=False,
    max_active_runs=1,
    tags=[env, 'opsi', 'replicate', 'prism'],
):
    repl_prism = SSHOperator(
        task_id=f"{env_pre}cOPSi_Repl_Prism",
        ssh_conn_id=SSH_CONN_ID,
        command=r"E:\\local\\OPSi\\proc\\OPSi_Repl_Prism.cmd"
    )
