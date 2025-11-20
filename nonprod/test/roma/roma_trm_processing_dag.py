from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))

# Import shared utilities
from utils.common_utils import get_environment_from_path

# Environment and connection configuration
ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]
app_name = os.path.basename(os.path.dirname(__file__))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 6),
}

with DAG(
        dag_id=f'{env_pre}d_roma_trm_processing',
        default_args=default_args,
        schedule=None,  # manual runs only
        catchup=False,
        params={'date': '20251006'},  # default date
        description='Run ROMA scripts via SSH in sequence',
) as dag:

    # Task 1: roma_preptrm
    roma_preptrm = SSHOperator(
        task_id=f'{env_pre}cROMA_preptrm',
        ssh_conn_id='RAXTestuser',
        command='/TEST/LIB/ROMA/ROMA_oper/proc/ROMA_preptrm.sh {{ params.date | default("20251006") }}',
        get_pty=True,
    )

    # Task 2: roma_cnvtrm
    roma_cnvtrm = SSHOperator(
        task_id=f'{env_pre}cROMA_cnvtrm',
        ssh_conn_id='RAXTestuser',
        command='/TEST/LIB/ROMA/ROMA_cnvtrm/proc/ROMA_cnvtrm.sh 5000',
        get_pty=True,
    )

    # Task 3: roma_mv2bptrm
    roma_mv2bptrm = SSHOperator(
        task_id=f'{env_pre}cROMA_mv2bptrm',
        ssh_conn_id='RAXTestuser',
        command='/TEST/LIB/ROMA/ROMA_oper/proc/ROMA_mv2bptrm.sh ',
        get_pty=True,
    )

    roma_preptrm >> roma_cnvtrm >> roma_mv2bptrm