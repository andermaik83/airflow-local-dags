from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from utils.common_utils import get_environment_from_path, resolve_connection_id

ENV = get_environment_from_path(__file__)
env = ENV.lower()
app_name = 'watch'
env_pre = env[0]  # 't' for test, etc.

# SSH Connection IDs 
SSH_CONN_ID = resolve_connection_id(ENV, "opr_vl101")
SSH_CONN_ID_2 = resolve_connection_id(ENV, "opr_vl111")
SSH_CONN_ID_3 = resolve_connection_id(ENV, "opr_vl113")

# Default arguments
WATCH_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'execution_timeout': timedelta(hours=4),
}

dag = DAG(
    dag_id=f'{env_pre}d_{app_name}_wtchdev_run',
    default_args=WATCH_DEFAULT_ARGS,
    description='Device WatchRun workflow',
    schedule='0 3 * * 2,4,6',  # tu,th,sa at 03:00
    catchup=False,
    max_active_runs=1,
    tags=[env, app_name,'wtchdev']
)

with TaskGroup('tbWTCHdev_Run', dag=dag) as wtchdev_run_group:
    wtchdev_watchrun = SSHOperator(
        task_id='tcWTCHdev_WatchRun',
        ssh_conn_id=SSH_CONN_ID_2,
        command=f'/{ENV}/LIB/WTCHdev/WTCHdev_watchrun/proc/WTCHdev_WatchRun.sh ',
        dag=dag,
    )
    wtchdev_colorwatch = SSHOperator(
        task_id='tcWTCHdev_ColorWatch',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/WTCHdev/WTCHdev_colorwtchfltr/proc/WTCHdev_ColorWatchFilter.sh ',
        dag=dag,
    )
    iris_batchorderupdate = SSHOperator(
        task_id='tcIRIS_batchorderupdate',
        ssh_conn_id=SSH_CONN_ID_2,
        command=f'/{ENV}/LIB/IRIS/IRIS_batchorderupdate/proc/IRIS_batchorderupdate.sh ',
        dag=dag,
    )
    mail_devgroup_succes = SSHOperator(
        task_id='tcWTCHdev_MailDevGroupSUCCES_CW',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/WTCHdev/WTCHdev_oper/proc/WTCHdev_MailDeviceGroupSUCCES_CW.sh ',
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    mail_devgroup_fail = SSHOperator(
        task_id='tcWTCHdev_MailDevGroupFAIL_CW',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/WTCHdev/WTCHdev_oper/proc/WTCHdev_MailDeviceGroupFAIL_CW.sh ',
        dag=dag,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    wtchdev_watchrun >> wtchdev_colorwatch >> iris_batchorderupdate
    wtchdev_colorwatch >> mail_devgroup_succes
    wtchdev_colorwatch >> mail_devgroup_fail

wtchdev_run_group
