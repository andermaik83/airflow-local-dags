"""
TEST Watch USA Common Law (CL) Workflow DAG
Complete CL workflow based on USA CL.txt JIL
File-triggered workflow for USA Common Law processing

This DAG represents the complete CA Autosys TEST_watch CL workflow that includes:
- File watching for CL COM files
- COMrec CL processing
- CTR Loader CL processing  
- WTCHwrd CL regional processing
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
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
app_name = 'watch'
env_pre = env[0]  # 't' for test, 'a' for acpt, 'p' for prod

# SSH Connection IDs - Environment aware
SSH_CONNECTIONS = {
    'LINUX_PRIMARY': 'tgen_vl101',    # tgen-vl101
    'LINUX_MONITOR': 'tgen_vl105',    # tgen-vl105  
    'WINDOWS_PRIMARY': 'topr_vw103',  # topr-vw103
}

# Default arguments
WATCH_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}


# DAG Definition
dag = DAG(
    dag_id=f'{env_pre}d_{app_name}_usa_cl',
    default_args=WATCH_DEFAULT_ARGS,
    description=f'TEST Watch USA Common Law (CL) Complete Workflow - {ENV}',
    schedule='*/5 * * * *',  # runs every 10 minutes, but never overlaps
    catchup=False,
    max_active_runs=1,
    tags=[env, app_name, 'cl-workflow'],
)

# ====== COMREC CL PROCESSING (tbCOMrec_CL) ======
# Original CA Box: tbCOMrec_CL - contains both file watcher and processing task
with TaskGroup(group_id=f'{env_pre}bCOMrec_CL', dag=dag) as comrec_cl_group:
    
    # tfCOMrec_CL - File watcher inside tbCOMrec_CL box (continuous monitoring)
    cl_com_file_sensor = SFTPSensor(
        task_id=f'{env_pre}fCOMrec_CL',
        sftp_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],  # tgen-vl105
        path=f'/{ENV}/SHR/COMrec/data/ke3/comCL',
        dag=dag,
        poke_interval=60,     # Check every 60 seconds
        timeout=timedelta(days=365 * 10), # Effectively never times out (e.g., 10 years)
        mode='reschedule',   # Don't block workers - reschedule when no file
        doc_md="""
        **COMrec CL File Watcher**
        
        **Purpose:**
        - Continuous monitoring for USA Common Law COM file creation
        - Original CA job: tfCOMrec_CL (File Trigger inside tbCOMrec_CL box)
        - Watch path: /TEST/SHR/COMrec/data/ke3/comCL
        - Watch type: CREATE with SIZE change
        - Wait time: 60S no change
        
        **Trigger Conditions:**
        - File creation detected
        - File size stabilized (no changes for 60 seconds)
        """
    )
    
    # tcCOMrec_CL - Main COMrec processing task for CL region
    comrec_cl = SSHOperator(
        task_id=f'{env_pre}cCOMrec_CL',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],  # tgen-vl105
        command=f'/{ENV}/LIB/COMrec/COMrec_CL/proc/COMrec_CL.sh',  # Assumed command path based on pattern
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **COMrec CL Processing**
        
        **Purpose:**
        - Process COMrec files for USA Common Law region
        - Original CA task: tcCOMrec_CL (inside tbCOMrec_CL box)
        - Triggered by: tfCOMrec_CL file watcher (within same box)
        - Region: CL (Common Law)
        - Application: COMrec
        - Machine: tgen-vl105
        
        **Processing:**
        - CL-specific COM file processing
        - USA Common Law trademark processing
        """
    )
    
    # Internal TaskGroup dependencies - file watcher triggers processing 
    cl_com_file_sensor >> comrec_cl

# ====== NVS COUNT OFFLOAD CL ======
# Original condition: s(tbCOMrec_CL)
nvs_offload_cl = SSHOperator(
    task_id=f'{env_pre}cNVScnt_offload_WTCH_CL',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],  # tgen-vl105
    command=f'/{ENV}/LIB/NVScnt/NVScnt_offload/proc/NVScnt_SaegisOffload_CTRWTCH_US.sh WATCH CL I',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **NVS Count Offload Watch CL**
    
    **Purpose:**
    - Download images for CTRldr (CL region)
    - Original CA job: tcNVScnt_offload_WTCH_CL
    - Parameters: WATCH CL I
    - Original condition: s(tbCOMrec_CL)
    - Application: NVScnt
    """
)

# ====== CTR LOADER XSL TRANSFORMER CL ======
# Original condition: s(tcNVScnt_offload_WTCH_CL)
ctr_xsl_transformer_cl = SSHOperator(
    task_id=f'{env_pre}cCTRldr_XSLtransformer_CL',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],  # tgen-vl105
    command=f'/{ENV}/LIB/CTRldr/CTRldr_XSLtransformer/proc/CTRldr_XSLtransformer.sh CL',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **CTR Loader XSL Transformer CL**
    
    **Purpose:**
    - Transform CTR data using XSL stylesheets (CL region)
    - Original CA job: tcCTRldr_XSLtransformer_CL
    - Parameters: CL
    - Original condition: s(tcNVScnt_offload_WTCH_CL)
    - Application: CTRldr
    """
)

# ====== CTR LOADER WATCH CL ======
# Original condition: s(tcCTRldr_XSLtransformer_CL)
ctr_wtch_cl = SSHOperator(
    task_id=f'{env_pre}cCTRldr_WTCH_CL',
    ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],  # tgen-vl105
    command=f'/{ENV}/LIB/CTRldr/CTRldr_WTCH/proc/CTRldr_WTCH.sh CL',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **CTR Loader Watch CL**
    
    **Purpose:**
    - Monitor CTR loader processes (CL region)
    - Original CA job: tcCTRldr_WTCH_CL
    - Parameters: CL
    - Original condition: s(tcCTRldr_XSLtransformer_CL)
    - Application: CTRldr
    """
)

# ====== WTCHWRD CL PROCESSING (tbWTCHwrd_CL) ======
# Original CA Box: tbWTCHwrd_CL with condition: s(tcCTRldr_WTCH_CL)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_CL', dag=dag) as wtchwrd_cl_group:
    
    # tcWTCHwrd_WatchHitComFil_TRMCL
    wtchwrd_watch_hit_comfil_trmcl = SSHOperator(
        task_id=f'{env_pre}cWTCHwrd_WatchHitComFil_TRMCL',
        ssh_conn_id=SSH_CONNECTIONS['LINUX_MONITOR'],  # tgen-vl105
        command=f'/{ENV}/LIB/WTCHwrd/WTCHwrd_hitcomfile/proc/WTCHwrd_WatchHitComfileTRM_TT.sh TRMCL CL',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Watch Hit COM File TRMCL**
        
        **Purpose:**
        - Monitor hit communication files for TRM CL region
        - Original CA job: tcWTCHwrd_WatchHitComFil_TRMCL (in tbWTCHwrd_CL box)
        - Parameters: TRMCL CL
        - Application: WTCHwrd
        """
    )
    
    # tcWTCHwrd_SetOrdToP_CL
    wtchwrd_setord_cl = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_CL',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],  # topr-vw103
        command=f'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd CLc',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Set Orders to Process CL**
        
        **Purpose:**
        - Set status orders to process for CL region
        - Original CA job: tcWTCHwrd_SetOrdToP_CL (in tbWTCHwrd_CL box)
        - Parameters: CLc
        - Original condition: s(tcWTCHwrd_WatchHitComFil_TRMCL)
        - Owner: test@INT
        """
    )
    
    # tcWTCHwrd_ProductionRun_CLc
    wtchwrd_production_cl = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_CLc',
        ssh_conn_id=SSH_CONNECTIONS['WINDOWS_PRIMARY'],  # topr-vw103
        command=f'E:\local\OPSi\proc\WTCHwrd_ProductionRunEvaTri.cmd CLc',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Production Run CLc**
        
        **Purpose:**
        - Execute production run for CL region with evaluation
        - Original CA job: tcWTCHwrd_ProductionRun_CLc (in tbWTCHwrd_CL box)
        - Parameters: CLc
        - Original condition: s(tcWTCHwrd_SetOrdToP_CL)
        - Command variant: ProductionRunEvaTri (vs ProductionRunEva in other regions)
        """
    )
    
    # Define tbWTCHwrd_CL internal dependencies
    wtchwrd_watch_hit_comfil_trmcl >> wtchwrd_setord_cl >> wtchwrd_production_cl

# ====== MAIN WORKFLOW DEPENDENCIES ======
# Based on the CA Autosys JIL cyclic dependency structure:

# 1. COMrec CL box (contains file watcher and processing) triggers NVS offload
comrec_cl_group >> nvs_offload_cl

# 2. Sequential CTR loader processing
nvs_offload_cl >> ctr_xsl_transformer_cl >> ctr_wtch_cl

# 3. CTR completion triggers WTCHwrd CL processing
ctr_wtch_cl >> wtchwrd_cl_group
