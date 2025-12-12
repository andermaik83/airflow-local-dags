from airflow import DAG
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from datetime import datetime, timedelta
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))

# Import shared utilities
from utils.common_utils import get_environment_from_path, resolve_connection_id

# Environment and connection configuration
ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]
app_name = os.path.basename(os.path.dirname(__file__))

SCRIPT_PATH = "e:\\local\\OPSi_monitor\\proc\\OPSi_monitor.cmd"
LOG_DIR = "E:\\Local\\OPSi_monitor\\log"

# WINRM Connection ID
WINRM_CONN_ID = resolve_connection_id(ENV,"opr_vw104")

default_args = {
    'email_on_failure': False,  # alarm_if_fail: 0 means no alerts
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id=f'{env_pre}d_bigb_opsi_monitor',
    default_args=default_args,
    description='OPSi Monitor job converted from Autosys - runs every 10 minutes, 7 days a week',
    schedule='*/10 * * * *',  # Every 10 minutes (00,10,20,30,40,50)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=["windows", "opsi", "monitor"],
) as dag:

    # Using WinRMOperator for Windows machine
    bigb_opsi_monitor = WinRMOperator(
        task_id=f'{env_pre}c_bigb_opsi_monitor',
        command=f'cd /d E:\\Local\\OPSi_monitor && {SCRIPT_PATH} 1>{LOG_DIR}\\output.log 2>{LOG_DIR}\\error.log',
        ssh_conn_id=WINRM_CONN_ID      
    )

