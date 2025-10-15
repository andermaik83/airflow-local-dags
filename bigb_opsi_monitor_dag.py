from airflow import DAG
from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from datetime import datetime, timedelta


SCRIPT_PATH = "e:\\local\\OPSi_monitor\\proc\\OPSi_monitor.cmd"
LOG_DIR = "E:\\Local\\OPSi_monitor\\log"
WINRM_CONN_ID = "topr-vw103" 

default_args = {
    'email_on_failure': False,  # alarm_if_fail: 0 means no alerts
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='tcBIGB_OPSi_monitor',
    default_args=default_args,
    description='OPSi Monitor job converted from Autosys - runs every 10 minutes, 7 days a week',
    schedule='*/10 * * * *',  # Every 10 minutes (00,10,20,30,40,50)
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=["windows", "opsi", "monitor"],
) as dag:

    # Using WinRMOperator for Windows machine
    opsi_monitor_task = WinRMOperator(
        task_id='run_opsi_monitor',
        conn_id=WINRM_CONN_ID,  # ✅ Changed from winrm_conn_id to conn_id
        command=f'cd /d E:\\Local\\OPSi_monitor && {SCRIPT_PATH} 1>{LOG_DIR}\\output.log 2>{LOG_DIR}\\error.log',  # ✅ Changed from list to string
        dag=dag,
    )

