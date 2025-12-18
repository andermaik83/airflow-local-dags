"""
TEST SEArun run I9 migrated from AutoSys to Airflow.
- Boxes modeled with TaskGroups
- Uses SSHOperator for command jobs
- File watcher tfSEArun_RunFile_I9 modeled as a placeholder sensor to be swapped when connection/path confirmed
"""
from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# Import shared utils (fallbacks if not available)
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
try:
    from utils.common_utils import get_environment_from_path, resolve_connection_id
except Exception:
    def get_environment_from_path(_file: str) -> str:
        return os.getenv('AIRFLOW_ENV', 'TEST').upper()
    def resolve_connection_id(env_name: str, logical: str) -> str:
        return {'tm_searun': 'tm-searun', 'tgen_vl101': 'tgen-vl101'}.get(logical, 'tm-searun')

ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]

SSH_MAIN = resolve_connection_id(ENV, 'tm_searun')
SSH_SPLIT = resolve_connection_id(ENV, 'tgen_vl101')

DEFAULT_ARGS = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def cmd_path(proc: str, *args: str) -> str:
    base = f"/{ENV}/LIB/SEArun/proc/{proc}"
    return base + (" " + " ".join(args) if args else "")

# Registers for I9 (example; extend with your list)
REGISTERS_I9 = [
    ("CZRE", 20, 10),
    ("SLRE", 20, 10),
    ("SLOV", 20, 10),
    ("HUNG", 20, 10),
]

with DAG(
    dag_id=f"{env_pre}d_searun_run_i9",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} SEArun run I9",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[env, 'searun', 'i9'],
) as dag:

    # tbSEArun_Run_I9
    wait_runfile = EmptyOperator(
        task_id=f"{env_pre}fSEArun_RunFile_I9",
        doc_md=(
            "Placeholder for AutoSys tfSEArun_RunFile_I9 sensor.\n"
            "Replace with FileSensor/SFTPSensor once path/connection are known."
        ),
    )

    rename_run = SSHOperator(
        task_id=f"{env_pre}cSEArun_RenameRun_I9",
        ssh_conn_id=SSH_MAIN,
        command=cmd_path('SEArun_RenameRun.sh', 'I9'),
        doc_md="Rename inputfile daily run",
    )

    with TaskGroup(group_id=f"{env_pre}bSEArun_Registers_I9", tooltip="Registers for I9") as tg_registers:
        for reg, job_load, priority in REGISTERS_I9:
            SSHOperator(
                task_id=f"{env_pre}cSEArun_Srch{reg}_I9",
                ssh_conn_id=SSH_MAIN,
                command=cmd_path('SEArun_SearchRun.sh', reg, 'I9'),
                doc_md=f"Search for {reg} in daily run",
            )

    copy_run = SSHOperator(
        task_id=f"{env_pre}cSEArun_CopyRun_I9",
        ssh_conn_id=SSH_MAIN,
        command=cmd_path('SEArun_CopyRun.sh', 'I9'),
        doc_md="Copy resultfile to OPS directory",
    )

    split_run = SSHOperator(
        task_id=f"{env_pre}cSEArun_SplitRun_I9",
        ssh_conn_id=SSH_SPLIT,
        command=cmd_path('SEArun_SplitRun.sh', 'I9'),
        doc_md="Start mvsbatch and opsbatch",
    )

    start_runfile = SSHOperator(
        task_id=f"{env_pre}eSEArun_StartRunFile_I9",
        ssh_conn_id=SSH_MAIN,
        command=f"echo 'sendevent -E FORCE_STARTJOB -J tfSEArun_RunFile_I9'",
        doc_md="Force filewatcher (placeholder)",
    )

    # Dependencies (align as needed with your final ordering)
    wait_runfile >> rename_run
    rename_run >> tg_registers
    tg_registers >> copy_run >> split_run >> start_runfile
