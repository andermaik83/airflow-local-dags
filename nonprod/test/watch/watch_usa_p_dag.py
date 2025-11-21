"""
TEST Watch USA PEND (UP) Workflow DAG
Derived from workflow_usa_p.jil (CA Autosys) preserving semantics.

Inside tbCOMrec_USAP box:
  - tfCOMrec_UP (file trigger on comUP.start)
  - tcCOMrec_remove_startfile_UP (remove start file)
  - tcCOMrec_BPenricher_UP (enrich COM file)
  - tcCOMrec_UP (COMrec expansion)

After box completion (s(tbCOMrec_USAP)) parallel roots:
  1. tcWTCHwrd_WatchHitComFil_TRMUP -> tbWTCHwrd_UP (SetOrdToP_UP -> ProductionRun_UPc)
                                     -> tbWTCHwrd_OW (SetOrdToP_OW -> ProductionRun_OW)
                                     -> tbWTCHwrd_SetOrdToP_UP_OG_Daily (SetOrdToP_UP_Daily -> SetOrdToP_OG_Daily)
  2. tcNVScnt_offload_WTCH_UP -> tcCTRldr_XSLtransformer_UP -> tcCTRldr_WTCH_UP
  3. tfCOMrec_check_comfile_UP -> tcCOMrec_check_comfile_UP

No explicit completion marker (as requested). Terminal tasks end the run.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from airflow.utils.task_group import TaskGroup
import os
import sys

# Import shared environment detection
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from utils.common_utils import get_environment_from_path

ENV = get_environment_from_path(__file__)
env = ENV.lower()
app_name = 'watch'
env_pre = env[0]  # 't' for TEST

SSH_CONNECTIONS = {
    'LINUX_MONITOR': 'tgen_vl105',
    'LINUX_PRIMARY': 'tgen_vl101',
    'WINDOWS_PRIMARY': 'topr_vw103',
}

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id=f'{env_pre}d_{app_name}_usa_p',
    default_args=DEFAULT_ARGS,
    description=f'TEST Watch USA PEND Workflow - {ENV}',
    schedule=None,  # manual or external trigger
    catchup=False,
    max_active_runs=1,
    tags=[env, app_name, 'usa-pend', 'watch'],
)

# ================= tbCOMrec_USAP BOX =================
with TaskGroup(group_id=f'{env_pre}bCOMrec_USAP', dag=dag) as comrec_usap_group:

    tf_comrec_up = SFTPSensor(
        task_id=f'{env_pre}fCOMrec_UP',
        sftp_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
        path=f'/{ENV}/SHR/COMrec/data/ke3/comUP.start',
        poke_interval=60,
        timeout=timedelta(days=180),
        mode='reschedule',
        doc_md="""**tfCOMrec_UP** Watches for comUP.start creation (CREATE + SIZE stable)."""
    )

    remove_startfile_up = SSHOperator(
        task_id=f'{env_pre}cCOMrec_remove_startfile_UP',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_PRIMARY'],
        command=f'rm -f /{ENV}/SHR/COMrec/data/ke3/comUP.start',
        dag=dag,
        doc_md="""**tcCOMrec_remove_startfile_UP** Removes trigger file (depends on tfCOMrec_UP)."""
    )

    bpenricher_up = SSHOperator(
        task_id=f'{env_pre}cCOMrec_BPenricher_UP',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
        command=f'/{ENV}/LIB/COMrec/COMrec_BPenricher/proc/COMrec_BPenricher_OG_UP.sh comUP UP',
        dag=dag,
        doc_md="""**tcCOMrec_BPenricher_UP** Adds comcode (depends on tfCOMrec_UP)."""
    )

    comrec_expand_up = SSHOperator(
        task_id=f'{env_pre}cCOMrec_UP',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
        command=f'/{ENV}/LIB/COMrec/COMrec_expand/proc/COMrec_Expand_US.sh UP',
        dag=dag,
        doc_md="""**tcCOMrec_UP** COMrec expansion (depends on tcCOMrec_BPenricher_UP)."""
    )

    tf_comrec_up >> [remove_startfile_up, bpenricher_up]
    bpenricher_up >> comrec_expand_up

# ================= Parallel Root 1: WTCHwrd WatchHitComFil =================
wtchwrd_watch_trmup = SSHOperator(
    task_id=f'{env_pre}cWTCHwrd_WatchHitComFil_TRMUP',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/WTCHwrd/WTCHwrd_hitcomfile/proc/WTCHwrd_WatchHitComfileTRM_TT.sh TRMUP UP',
    dag=dag,
    doc_md="""**tcWTCHwrd_WatchHitComFil_TRMUP** Initial WTCHwrd trigger (condition s(tbCOMrec_USAP))."""
)

# WTCHwrd UP box
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_UP', dag=dag) as wtchwrd_up_group:
    wtchwrd_setord_up = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_UP',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=f'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd UP',
        dag=dag,
        doc_md="""**tcWTCHwrd_SetOrdToP_UP** Depends on tcWTCHwrd_WatchHitComFil_TRMUP."""
    )
    wtchwrd_prod_up = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_UPc',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=f'E:\local\OPSi\proc\WTCHwrd_ProductionRunEva.cmd UPc',
        dag=dag,
        doc_md="""**tcWTCHwrd_ProductionRun_UPc** Depends on tcWTCHwrd_SetOrdToP_UP."""
    )
    wtchwrd_setord_up >> wtchwrd_prod_up

# WTCHwrd OW box
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_OW', dag=dag) as wtchwrd_ow_group:
    wtchwrd_setord_ow = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_OW',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=f'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd OWc',
        dag=dag,
        doc_md="""**tcWTCHwrd_SetOrdToP_OW** Depends on tcWTCHwrd_WatchHitComFil_TRMUP."""
    )
    wtchwrd_prod_ow = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_OW',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=f'E:\local\OPSi\proc\WTCHwrd_ProductionRunEva.cmd OWc',
        dag=dag,
        doc_md="""**tcWTCHwrd_ProductionRun_OW** Depends on tcWTCHwrd_SetOrdToP_OW."""
    )
    wtchwrd_setord_ow >> wtchwrd_prod_ow

# Daily UP/OG box (simplified linear dependency inside group)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_SetOrdToP_UP_OG_Daily', dag=dag) as daily_up_og_group:
    wtchwrd_setord_up_daily = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_UP_Daily',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=f'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd UP',
        dag=dag,
        doc_md="""**tcWTCHwrd_SetOrdToP_UP_Daily** Daily UP orders."""
    )
    wtchwrd_setord_og_daily = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_OG_Daily',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=f'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd OG',
        dag=dag,
        doc_md="""**tcWTCHwrd_SetOrdToP_OG_Daily** Daily OG orders (depends on UP Daily)."""
    )
    wtchwrd_setord_up_daily >> wtchwrd_setord_og_daily

# ================= Parallel Root 2: NVScnt / CTR Loader Chain =================
nvscnt_offload_up = SSHOperator(
    task_id=f'{env_pre}cNVScnt_offload_WTCH_UP',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/NVScnt/NVScnt_offload/proc/NVScnt_SaegisOffload_CTRWTCH_US.sh WATCH UP I',
    dag=dag,
    doc_md="""**tcNVScnt_offload_WTCH_UP** Condition s(tbCOMrec_USAP)."""
)
ctr_xsl_up = SSHOperator(
    task_id=f'{env_pre}cCTRldr_XSLtransformer_UP',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/CTRldr/CTRldr_XSLtransformer/proc/CTRldr_XSLtransformer.sh UP',
    dag=dag,
    doc_md="""**tcCTRldr_XSLtransformer_UP** Depends on tcNVScnt_offload_WTCH_UP."""
)
ctr_wtch_up = SSHOperator(
    task_id=f'{env_pre}cCTRldr_WTCH_UP',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/CTRldr/CTRldr_WTCH/proc/CTRldr_WTCH.sh UP',
    dag=dag,
    doc_md="""**tcCTRldr_WTCH_UP** Depends on tcCTRldr_XSLtransformer_UP."""
)

nvscnt_offload_up >> ctr_xsl_up >> ctr_wtch_up

# ================= Parallel Root 3: COM file check =================
check_comfile_sensor_up = SFTPSensor(
    task_id=f'{env_pre}fCOMrec_check_comfile_UP',
    sftp_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    path=f'/{ENV}/SHR/COMrec/data/ke3/comUP',
    poke_interval=30,
    timeout=timedelta(days=30),
    mode='reschedule',
    doc_md="""**tfCOMrec_check_comfile_UP** Post-box file presence check (CREATE + SIZE)."""
)
comrec_check_comfile_up = SSHOperator(
    task_id=f'{env_pre}cCOMrec_check_comfile_UP',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/COMrec/COMrec_oper/proc/COMrec_Checkcomfile_UP_OG.sh UP',
    dag=dag,
    doc_md="""**tcCOMrec_check_comfile_UP** Depends on tfCOMrec_check_comfile_UP."""
)
check_comfile_sensor_up >> comrec_check_comfile_up

# ================= Fan-out from COMrec box completion =================
comrec_usap_group >> [
    wtchwrd_watch_trmup,
    nvscnt_offload_up,
    check_comfile_sensor_up,
]

# WTCHwrd watch triggers boxes + daily group
# WTCHwrd watch triggers UP and OW boxes only (daily group independent of watch trigger)
wtchwrd_watch_trmup >> [wtchwrd_up_group, wtchwrd_ow_group]
# Daily UP must also depend on regular SetOrdToP_UP completion per JIL predecessor semantics
wtchwrd_setord_up >> wtchwrd_setord_up_daily

# Terminal tasks are the ends of each branch (no completion marker per request).
