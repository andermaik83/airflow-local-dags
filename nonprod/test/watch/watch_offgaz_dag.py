"""
TEST Watch Official Gazette (OG) Workflow DAG
Recreated from `workflow_tbCOMrec_OffGaz.jil` preserving Autosys semantics:

Inside box tbCOMrec_OffGaz:
  - tfCOMrec_OG (file trigger on comOG.start)
  - tcCOMrec_remove_startfile_OG (remove start file)
  - tcCOMrec_BPenricher_OG (enrich COM file)
  - tcCOMrec_OG (COMrec expansion)

After box completion (s(tbCOMrec_OffGaz)) three parallel branches start:
  1. NVScnt offload chain: tcNVScnt_offload_WTCH_OG -> tcCTRldr_XSLtransformer_OG -> tcCTRldr_WTCH_OG
  2. WTCHwrd branch: tcWTCHwrd_WatchHitComFil_TRMOG -> tbWTCHwrd_OG (tcWTCHwrd_SetOrdToP_OG -> tcWTCHwrd_ProductionRun_OGc)
  3. COM file check: tfCOMrec_check_comfile_OG -> tcCOMrec_check_comfile_OG

No cyclic edge back to the COMrec box; scheduling controls recurrence.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import os
import sys

# Utility import path for common utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from utils.common_utils import get_environment_from_path

ENV = get_environment_from_path(__file__)
env = ENV.lower()
app_name = 'watch'
env_pre = env[0]  # t / a / p

SSH_CONNECTIONS = {
    'LINUX_PRIMARY': 'tgen_vl101',    # tgen-vl101
    'LINUX_MONITOR': 'tgen_vl105',    # tgen-vl105
    'WINDOWS_PRIMARY': 'topr_vw103',  # topr-vw103
}

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

dag = DAG(
    dag_id=f'{env_pre}d_{app_name}_offgaz',
    default_args=DEFAULT_ARGS,
    description=f'TEST Watch Official Gazette Workflow - {ENV}',
    schedule='*/15 * * * *',  # periodic scan; adjust as needed
    catchup=False,
    max_active_runs=1,
    tags=[env, app_name, 'offgaz-workflow'],
)

# =============== tbCOMrec_OffGaz BOX ===============
with TaskGroup(group_id=f'{env_pre}bCOMrec_OffGaz', dag=dag) as comrec_offgaz_group:

    tf_comrec_og = SFTPSensor(
        task_id=f'{env_pre}fCOMrec_OG',
        sftp_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
        path=f'/{ENV}/SHR/COMrec/data/ke3/comOG.start',
        poke_interval=60,
        timeout=timedelta(days=365),
        mode='reschedule',
        doc_md="""**tfCOMrec_OG** Watches for comOG.start creation (CREATE + SIZE stable)."""
    )

    remove_startfile = SSHOperator(
        task_id=f'{env_pre}cCOMrec_remove_startfile_OG',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
        command=f'rm -f /{ENV}/SHR/COMrec/data/ke3/comOG.start',
        dag=dag,
        doc_md="""**tcCOMrec_remove_startfile_OG** Removes trigger file after detection (depends on tfCOMrec_OG)."""
    )

    bpenricher_og = SSHOperator(
        task_id=f'{env_pre}cCOMrec_BPenricher_OG',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
        command=f'/{ENV}/LIB/COMrec/COMrec_BPenricher/proc/COMrec_BPenricher_OG_UP.sh comOG OG',
        dag=dag,
        doc_md="""**tcCOMrec_BPenricher_OG** Adds comcode (depends on tfCOMrec_OG)."""
    )

    comrec_expand_og = SSHOperator(
        task_id=f'{env_pre}cCOMrec_OG',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
        command=f'/{ENV}/LIB/COMrec/COMrec_expand/proc/COMrec_Expand_US.sh OG',
        dag=dag,
        doc_md="""**tcCOMrec_OG** COMrec expansion (depends on tcCOMrec_BPenricher_OG)."""
    )

    # Internal dependencies
    tf_comrec_og >> [remove_startfile, bpenricher_og]
    bpenricher_og >> comrec_expand_og

# =============== Parallel Branch 1: NVScnt -> CTRldr Chain ===============
nvscnt_offload_og = SSHOperator(
    task_id=f'{env_pre}cNVScnt_offload_WTCH_OG',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/NVScnt/NVScnt_offload/proc/NVScnt_SaegisOffload_CTRWTCH_US.sh WATCH OG I',
    dag=dag,
    doc_md="""**tcNVScnt_offload_WTCH_OG** (condition s(tbCOMrec_OffGaz)) Downloads images for CTR loader."""
)

ctr_xsl_og = SSHOperator(
    task_id=f'{env_pre}cCTRldr_XSLtransformer_OG',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/CTRldr/CTRldr_XSLtransformer/proc/CTRldr_XSLtransformer.sh OG',
    dag=dag,
    doc_md="""**tcCTRldr_XSLtransformer_OG** (depends on tcNVScnt_offload_WTCH_OG)."""
)

ctr_wtch_og = SSHOperator(
    task_id=f'{env_pre}cCTRldr_WTCH_OG',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/CTRldr/CTRldr_WTCH/proc/CTRldr_WTCH.sh OG',
    dag=dag,
    doc_md="""**tcCTRldr_WTCH_OG** (depends on tcCTRldr_XSLtransformer_OG)."""
)

# =============== Parallel Branch 2: WTCHwrd ===============
wtchwrd_watch_trmog = SSHOperator(
    task_id=f'{env_pre}cWTCHwrd_WatchHitComFil_TRMOG',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/WTCHwrd/WTCHwrd_hitcomfile/proc/WTCHwrd_WatchHitComfileTRM_TT.sh TRMOG OG',
    dag=dag,
    doc_md="""**tcWTCHwrd_WatchHitComFil_TRMOG** (condition s(tbCOMrec_OffGaz)) initial WTCHwrd step."""
)

with TaskGroup(group_id=f'{env_pre}bWTCHwrd_OG', dag=dag) as wtchwrd_og_group:

    wtchwrd_setord_og = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_OG',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd OG',
        dag=dag,
        doc_md="""**tcWTCHwrd_SetOrdToP_OG** (depends on tcWTCHwrd_WatchHitComFil_TRMOG)."""
    )

    wtchwrd_production_og = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_OGc',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],
        command=r'E:\local\OPSi\proc\WTCHwrd_ProductionRunEva.cmd OGc',
        dag=dag,
        doc_md="""**tcWTCHwrd_ProductionRun_OGc** (depends on tcWTCHwrd_SetOrdToP_OG)."""
    )

    wtchwrd_setord_og >> wtchwrd_production_og

# =============== Parallel Branch 3: COM file check ===============
check_comfile_sensor = SFTPSensor(
    task_id=f'{env_pre}fCOMrec_check_comfile_OG',
    sftp_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    path=f'/{ENV}/SHR/COMrec/data/ke3/comOG',
    poke_interval=30,
    timeout=timedelta(days=30),
    mode='reschedule',
    doc_md="""**tfCOMrec_check_comfile_OG** (condition s(tbCOMrec_OffGaz)) monitors comOG file."""
)

comrec_check_comfile_og = SSHOperator(
    task_id=f'{env_pre}cCOMrec_check_comfile_OG',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],
    command=f'/{ENV}/LIB/COMrec/COMrec_oper/proc/COMrec_Checkcomfile_UP_OG.sh OG',
    dag=dag,
    doc_md="""**tcCOMrec_check_comfile_OG** (depends on tfCOMrec_check_comfile_OG)."""
)


# =============== Dependencies ===============
# Box completion fans out to three roots
comrec_offgaz_group >> [nvscnt_offload_og, wtchwrd_watch_trmog, check_comfile_sensor]

# NVScnt chain
nvscnt_offload_og >> ctr_xsl_og >> ctr_wtch_og

# WTCHwrd chain
wtchwrd_watch_trmog >> wtchwrd_og_group

# COM file check chain
check_comfile_sensor >> comrec_check_comfile_og
