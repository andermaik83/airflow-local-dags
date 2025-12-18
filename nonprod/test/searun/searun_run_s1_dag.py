"""
TEST SEArun run S1 migrated from AutoSys to Airflow.
- Boxes modeled with TaskGroups
- Uses SSHOperator for command jobs
- File watcher tfSEArun_RunFile_S1 modeled as a placeholder sensor to be swapped when connection/path confirmed
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

# Registers per the snippet (expand as needed)
REGISTERS_S1 = [
    ("ALGE", 10, 25),
    ("CTMP", 10, 10),
]

with DAG(
    dag_id=f"{env_pre}d_searun_run_s1",
    default_args=DEFAULT_ARGS,
    description=f"{ENV} SEArun run S1",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[env, 'searun', 's1'],
) as dag:

    # tbSEArun_Run_S1
    wait_runfile = EmptyOperator(
        task_id=f"{env_pre}fSEArun_RunFile_S1",
        doc_md=(
            "AutoSys tfSEArun_RunFile_S1 watches /TEST/SHR/SEArun/data/run/RUN1.\n"
            "Replace with FileSensor/SFTPSensor once connection is confirmed."
        ),
    )

    rename_run = SSHOperator(
        task_id=f"{env_pre}cSEArun_RenameRun_S1",
        ssh_conn_id=SSH_MAIN,
        command=cmd_path('SEArun_RenameRun.sh', 'S1'),
        doc_md="Rename inputfile daily run",
    )

    # Registers box depends on RenameRun
    with TaskGroup(group_id=f"{env_pre}bSEArun_Registers_S1", tooltip="Registers for S1") as tg_registers:
        search_tasks = []
        for reg, job_load, priority in REGISTERS_S1:
            t = SSHOperator(
                task_id=f"{env_pre}cSEArun_Srch{reg}_S1",
                ssh_conn_id=SSH_MAIN,
                command=cmd_path('SEArun_SearchRun.sh', reg, 'S1'),
                doc_md=f"Search for {reg} in daily run",
            )
            search_tasks.append(t)

    chk_regi = SSHOperator(
        task_id=f"{env_pre}cSEArun_ChkRegi_S1",
        ssh_conn_id=SSH_MAIN,
        command=cmd_path('SEArun_ChkRegi.sh', 'S1'),
        doc_md="Controleren van de hitfiles op inhoud",
    )

    sort_run = SSHOperator(
        task_id=f"{env_pre}cSEArun_SortRun_S1",
        ssh_conn_id=SSH_MAIN,
        command=cmd_path('SEArun_SortRun.sh', 'S1'),
        doc_md="Sorteren van de search files",
    )

    copy_run = SSHOperator(
        task_id=f"{env_pre}cSEArun_CopyRun_S1",
        ssh_conn_id=SSH_MAIN,
        command=cmd_path('SEArun_CopyRun.sh', 'S1'),
        doc_md="Copy resultfile to OPS directory",
    )

    split_run = SSHOperator(
        task_id=f"{env_pre}cSEArun_SplitRun_S1",
        ssh_conn_id=SSH_SPLIT,
        command=cmd_path('SEArun_SplitRun.sh', 'S1'),
        doc_md="Start mvsbatch and opsbatch",
    )

    start_chk = SSHOperator(
        task_id=f"{env_pre}eSEArun_StartChk_S1",
        ssh_conn_id=SSH_MAIN,
        command="echo 'sendevent -E FORCE_STARTJOB -J tbSEArun_Chk_S1'",
        doc_md="Force box tbSEArun_Chk_S1 (placeholder)",
    )

    # Dependencies per JIL
    wait_runfile >> rename_run
    rename_run >> tg_registers
    tg_registers >> chk_regi >> sort_run >> copy_run >> split_run >> start_chk
