from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 6),
}

with DAG(
        dag_id='roma_ssh_execution_dag_no_jinja',
        default_args=default_args,
        schedule=None,  # manual runs only
        catchup=False,
        params={'date': '20251006'},  # default date
        description='Run ROMA scripts via SSH in sequence',
) as dag:

    # Task 1: tcROMA_preptrm
    tcROMA_preptrm = SSHOperator(
        task_id='tcROMA_preptrm',
        ssh_conn_id='RAXTestuser',
        command='/TEST/LIB/ROMA/ROMA_oper/proc/ROMA_preptrm.sh {{ params.date | default("20251006") }}',
        get_pty=True,
    )

    # Task 2: tcROMA_cnvtrm
    tcROMA_cnvtrm = SSHOperator(
        task_id='tcROMA_cnvtrm',
        ssh_conn_id='RAXTestuser',
        command='/TEST/LIB/ROMA/ROMA_cnvtrm/proc/ROMA_cnvtrm.sh 5000',
        get_pty=True,
    )

    # Task 3: tcROMA_mv2bptrm
    tcROMA_mv2bptrm = SSHOperator(
        task_id='tcROMA_mv2bptrm',
        ssh_conn_id='RAXTestuser',
        command='/TEST/LIB/ROMA/ROMA_oper/proc/ROMA_mv2bptrm.sh ',
        get_pty=True,
    )

    tcROMA_preptrm >> tcROMA_cnvtrm >> tcROMA_mv2bptrm