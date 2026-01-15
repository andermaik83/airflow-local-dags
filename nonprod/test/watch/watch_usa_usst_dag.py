"""
TEST Watch USA USST Workflow DAG
Complete USST workflow based on USA_USAST.txt JIL
File-triggered workflow for USA USST processing (PEND Com jobs)

This DAG represents the CA Autosys TEST_watch USST workflow including:
- File watching for USST COM files (tfCOMrec_USST)
- COMrec USST processing (tcCOMrec_USST)
- NVS image offload (tcNVScnt_offload_WTCH_USST)
- CTR Loader transformation & watch (tcCTRldr_XSLtransformer_USST, tcCTRldr_WTCH_USST)
- WTCHwrd regional processing (tcWTCHwrd_WatchHitComFil_TRMUSST, tcWTCHwrd_SetOrdToP_USST, tcWTCHwrd_ProductionRun_USSTc)

"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from airflow.providers.standard.operators.empty import EmptyOperator
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
SSH_CONN_ID = resolve_connection_id(ENV, "opr_vl113")
WINRM_CONN_ID = resolve_connection_id(ENV, "opr_vw104")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'execution_timeout': timedelta(hours=4),
}

dag = DAG(
    dag_id=f'{env_pre}d_{app_name}_usa_usst',
    default_args=DEFAULT_ARGS,
    description=f'TEST Watch USA USST Workflow - {ENV}',
    schedule=None,  # manual / external trigger; add cron for periodic checks if desired
    catchup=False,
    max_active_runs=1,
    tags=[env, app_name, 'usst-workflow', 'file-triggered', 'sequential'],
)

# ====== COMREC USST BOX (tbCOMrec_USST) ======
with TaskGroup(group_id=f'{env_pre}bCOMrec_USST', dag=dag) as comrec_usst_group:

    # File watcher tfCOMrec_USST inside box
    usst_file_sensor = SFTPSensor(
        task_id=f'{env_pre}fCOMrec_USST',
        sftp_conn_id=SSH_CONN_ID,
        path=f'/{ENV}/SHR/COMrec/data/ke3/comUSST',
        poke_interval=60,
        timeout=timedelta(days=365),  # long running watch
        mode='reschedule',
        doc_md="""
        **COMrec USST File Watcher (tfCOMrec_USST)**
        Watches for creation of /TEST/SHR/COMrec/data/ke3/comUSST with size stabilization.
        Original JIL: continuous=0 (one-shot per run). Airflow run ends after downstream completes.
        """
    )

    # Processing task tcCOMrec_USST
    comrec_usst = SSHOperator(
        task_id=f'{env_pre}cCOMrec_USST',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/COMrec/COMrec_expand/proc/COMrec_Expand_US.sh USST',
        email_on_failure=True,
        dag=dag,
        doc_md="""
        **COMrec USST Processing (tcCOMrec_USST)**
        Command: COMrec_Expand_US.sh USST
        Condition in JIL: s(tfCOMrec_USST)
        """
    )

    # Internal dependency: sensor -> processing
    usst_file_sensor >> comrec_usst

# ====== NVS Offload (tcNVScnt_offload_WTCH_USST) ======
nvs_offload_usst = SSHOperator(
    task_id=f'{env_pre}cNVScnt_offload_WTCH_USST',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/NVScnt/NVScnt_offload/proc/NVScnt_SaegisOffload_CTRWTCH_US.sh WATCH USST I',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **NVS Count Offload USST**
    Downloads images for CTR loader (WATCH USST I)
    Condition: s(tbCOMrec_USST)
    """
)

# ====== CTR Loader XSL Transformer (tcCTRldr_XSLtransformer_USST) ======
ctr_xsl_usst = SSHOperator(
    task_id=f'{env_pre}cCTRldr_XSLtransformer_USST',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/CTRldr/CTRldr_XSLtransformer/proc/CTRldr_XSLtransformer.sh USST',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **CTR Loader XSL Transformer USST**
    Condition: s(tcNVScnt_offload_WTCH_USST)
    """
)

# ====== CTR Loader Watch (tcCTRldr_WTCH_USST) ======
ctr_wtch_usst = SSHOperator(
    task_id=f'{env_pre}cCTRldr_WTCH_USST',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/CTRldr/CTRldr_WTCH/proc/CTRldr_WTCH.sh USST',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **CTR Loader Watch USST**
    Condition: s(tcCTRldr_XSLtransformer_USST)
    """
)

# ====== WTCHwrd USST BOX (tbWTCHwrd_USST) ======
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_USST', dag=dag) as wtchwrd_usst_group:

    wtchwrd_watch_trm_usst = SSHOperator(
        task_id=f'{env_pre}cWTCHwrd_WatchHitComFil_TRMUSST',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/WTCHwrd/WTCHwrd_hitcomfile/proc/WTCHwrd_WatchHitComfileTRM_TT.sh TRMUSST USST',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Watch Hit COM File TRMUSST**
        First task in tbWTCHwrd_USST box.
        """
    )

    wtchwrd_setord_usst = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_USST',
        ssh_conn_id=WINRM_CONN_ID,
        command=f'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd STc',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Set Orders To Process USST**
        Condition: s(tcWTCHwrd_WatchHitComFil_TRMUSST)
        """
    )

    wtchwrd_prod_usst = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_USSTc',
        ssh_conn_id=WINRM_CONN_ID,
        command=f'E:\local\OPSi\proc\WTCHwrd_ProductionRunEvaTri.cmd STc',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Production Run USSTc**
        Condition: s(tcWTCHwrd_SetOrdToP_USST)
        Uses EvaTri variant.
        """
    )

    wtchwrd_watch_trm_usst >> wtchwrd_setord_usst >> wtchwrd_prod_usst


# ====== LINEAR DEPENDENCIES (NO CYCLE) ======
comrec_usst_group >> nvs_offload_usst >> ctr_xsl_usst >> ctr_wtch_usst >> wtchwrd_usst_group
