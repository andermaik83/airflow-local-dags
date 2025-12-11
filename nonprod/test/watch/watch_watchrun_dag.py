"""
TEST Watch WatchRun Workflow DAG
Complete WatchRun workflow based on com_watchrun.txt JIL
Triggered by main workflow completion (condition: s(tcATRIUM_MvComfile))

This DAG represents the complete CA Autosys TEST_watch workflow that includes:
- COMrec processing (tbCOMrec box)
- CTR Loader processing (tbCTRldr_load box)
- ATRIUM quality processing
- WTCHwrd hit monitoring
- WTCHdev processing
- Regional WTCHwrd processing (UP, OG, AW, WW, TT, WWL, Manual)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
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
from utils.common_utils import get_environment_from_path, resolve_connection_id

# Environment and connection configuration
ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]
app_name = os.path.basename(os.path.dirname(__file__))

# SSH Connection IDs 
SSH_CONN_ID = resolve_connection_id(ENV, "opr_vl113")
WINRM_CONN_ID = resolve_connection_id(ENV, "opr_vw104")

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

def get_common_tags(application, environment, additional_tags=None):
    """Generate common tags for DAGs"""
    tags = [f'{environment.upper()}', application, 'ca-autosys-migration']
    if additional_tags:
        tags.extend(additional_tags)
    return tags

# DAG Definition
dag = DAG(
    dag_id=f'{env_pre}d_{app_name}_watchrun',
    default_args=WATCH_DEFAULT_ARGS,
    description=f'TEST Watch WatchRun Comcplete Workflow - {ENV}',
    schedule=None,  # Triggered by main workflow via TriggerDagRunOperator
    catchup=False,
    max_active_runs=1,
    tags=[env, app_name,'watchrun']
)

# ====== ATRIUM MV COMFILE - INITIAL TRIGGER ======
atrium_mvcomfile = SSHOperator(
    task_id=f'{env_pre}cATRIUM_MvComfile',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/ATRIUM/ATRIUM_mvfile/proc/ATRIUM_mvcomfile.sh ',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **ATRIUM Move COM File - Initial Trigger**
    
    **Purpose:**
    - Initial trigger task for the entire WatchRun workflow
    - Adapted to: tcATRIUM_MvComfile (TEST environment)
    - Machine: tgen-vl101
    - Application: ATRIUM
    
    """
)

# ====== COMREC PROCESSING (tbCOMrec) ======
# Original CA Box: tbCOMrec with condition: s(tcATRIUM_MvComfile)
with TaskGroup(group_id=f'{env_pre}bCOMrec', dag=dag) as comrec_group:
    
    # tcCOMrec_BPenricher - Initial task in tbCOMrec box
    comrec_bpenricher = SSHOperator(
        task_id=f'{env_pre}cCOMrec_BPenricher',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/COMrec/COMrec_BPenricher/proc/COMrec_BPenricher.sh COMrec05 WW',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **COMrec BP Enricher - WatchRun**
        
        **Purpose:**
        - Add comcode for USA marks
        - Original CA job: tcCOMrec_BPenricher (in tbCOMrec box)
        - Parameters: COMrec05 WW
        - Machine: tgen-vl105
        - Owner: test
        - Application: COMrec
        """
    )
    
    # tcCOMrec_expand
    comrec_expand = SSHOperator(
        task_id=f'{env_pre}cCOMrec_expand',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/COMrec/COMrec_expand/proc/COMrec_Expand.sh ',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **COMrec Expand - WatchRun**
        
        **Purpose:**
        - Expand COMrec files
        - Original CA job: tcCOMrec_expand
        - Original condition: s(tcCOMrec_BPenricher)
        """
    )
    
    # tcCOMrec_Validate_XML
    comrec_validate_xml = SSHOperator(
        task_id=f'{env_pre}cCOMrec_Validate_XML',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/COMrec/COMrec_oper/proc/COMrec_Validate_XML.sh ',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **COMrec Validate XML - WatchRun**
        
        **Purpose:**
        - Validate COMrec XML files
        - Original CA job: tcCOMrec_Validate_XML
        - Original condition: s(tcCOMrec_expand)
        """
    )
    
    # tcCOMrec_DefComfileDB
    comrec_def_comfile_db = SSHOperator(
        task_id=f'{env_pre}cCOMrec_DefComfileDB',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/COMrec/COMrec_oper/proc/COMrec_DefineComfileinDB.sh ',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **COMrec Define Comfile in DB - WatchRun**
        
        **Purpose:**
        - Define communication files in database
        - Original CA job: tcCOMrec_DefComfileDB
        - Original condition: s(tcCOMrec_Validate_XML)
        """
    )
    
    # tcCOMrec_Statnewtrm
    comrec_statnewtrm = SSHOperator(
        task_id=f'{env_pre}cCOMrec_Statnewtrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/COMrec/COMrec_newmarks/proc/COMrec_statnewtrm.sh ',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **COMrec Stat New TRM - WatchRun**
        
        **Purpose:**
        - Statistics for new TRM records
        - Original CA job: tcCOMrec_Statnewtrm
        - Original condition: s(tcCOMrec_Validate_XML)
        """
    )
    
    # tcCOMrec_Mvcomfiles
    comrec_mvcomfiles = SSHOperator(
        task_id=f'{env_pre}cCOMrec_Mvcomfiles',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/COMrec/COMrec_oper/proc/COMrec_MvFiles.sh ',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **COMrec Move COM Files - WatchRun**
        
        **Purpose:**
        - Move communication files
        - Original CA job: tcCOMrec_Mvcomfiles
        - Original condition: s(tcCOMrec_Statnewtrm) & s(tcCOMrec_DefComfileDB)
        """
    )
    
    # Define tbCOMrec internal dependencies (from JIL analysis)
    comrec_bpenricher >> comrec_expand >> comrec_validate_xml
    comrec_validate_xml >> [comrec_def_comfile_db, comrec_statnewtrm]
    [comrec_def_comfile_db, comrec_statnewtrm] >> comrec_mvcomfiles

# ====== CTR LOADER PROCESSING (tbCTRldr_load) ======
# Original Box: tbCTRldr_load with condition: s(tbCOMrec)
with TaskGroup(group_id=f'{env_pre}bCTRldr_load', dag=dag) as ctr_loader_group:
    
    # tcNVScnt_offload_WTCH
    nvs_offload_wtch = SSHOperator(
        task_id=f'{env_pre}cNVScnt_offload_WTCH',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/NVScnt/NVScnt_offload/proc/NVScnt_SaegisOffload_CTRWTCH.sh WATCH I WW',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **NVS Count Offload Watch**
        
        **Purpose:**
        - Download images for CTRldr
        - Original CA job: tcNVScnt_offload_WTCH (in tbCTRldr_load box)
        - Parameters: WATCH I WW
        - Application: NVScnt
        """
    )
    
    # tcCTRldr_XSLtransformer
    ctr_xsl_transformer = SSHOperator(
        task_id=f'{env_pre}cCTRldr_XSLtransformer',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/CTRldr/CTRldr_XSLtransformer/proc/CTRldr_XSLtransformer.sh WW',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **CTR Loader XSL Transformer**
        
        **Purpose:**
        - Transform CTR data using XSL stylesheets
        - Original CA job: tcCTRldr_XSLtransformer
        - Original condition: s(tcNVScnt_offload_WTCH)
        - Parameters: WW
        """
    )
    
    # tcCTRldr_WTCH
    ctr_wtch = SSHOperator(
        task_id=f'{env_pre}cCTRldr_WTCH',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/CTRldr/CTRldr_WTCH/proc/CTRldr_WTCH.sh WW',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **CTR Loader Watch**
        
        **Purpose:**
        - Monitor CTR loader processes
        - Original CA job: tcCTRldr_WTCH
        - Original condition: s(tcCTRldr_XSLtransformer)
        - Parameters: WW
        """
    )
    
    # Define CTR loader internal dependencies
    nvs_offload_wtch >> ctr_xsl_transformer >> ctr_wtch

# ====== ATRIUM QUALITY PROCESSING ======
# Original condition: s(tbCOMrec)
atrium_qlty_comrec = SSHOperator(
    task_id=f'{env_pre}cATRIUM_Qlty_COMrec',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/COMrec/COMrec_oper/proc/COMrec_cptoAtrium.sh -d 4',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **ATRIUM Quality COMrec**
    
    **Purpose:**
    - Copy COMrec file to ATRIUM quality
    - Original CA job: tcATRIUM_Qlty_COMrec
    - Original condition: s(tbCOMrec)
    - Machine: tgen-vl101
    - Application: ATRIUM
    """
)

# ====== ATRIUM KE3 TOOL ======
# Original condition: s(tbCOMrec)
atrium_ke3tool = SSHOperator(
    task_id=f'{env_pre}cATRIUM_ke3tool',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/ATRIUM/ATRIUM_ke3tool/proc/ATRIUM_ke3tool.sh WKERROR.DTA',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **ATRIUM KE3 Tool**
    
    **Purpose:**
    - Process ATRIUM KE3 tool with error data
    - Original CA job: tcATRIUM_ke3tool
    - Original condition: s(tbCOMrec)
    - Parameters: WKERROR.DTA
    - Application: ATRIUM
    """
)

# ====== WTCHWRD HIT COM FILE MONITORING ======
# Original condition: s(tbCOMrec)
wtchwrd_watchhitcomfile_trm = SSHOperator(
    task_id=f'{env_pre}cWTCHwrd_WatchHitComFile_TRM',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/WTCHwrd/WTCHwrd_hitcomfile/proc/WTCHwrd_WatchHitComfileTRM.sh TRM',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **WTCHwrd Watch Hit COM File TRM**
    
    **Purpose:**
    - Monitor hit communication files for TRM
    - Original CA job: tcWTCHwrd_WatchHitComFile_TRM
    - Original condition: s(tbCOMrec)
    - Application: WTCHwrd
    """
)

# ====== WTCHDEV STORE HITS ======
# Original condition: s(tcWTCHwrd_WatchHitComFile_TRM)
wtchdev_storehits = SSHOperator(
    task_id=f'{env_pre}cWTCHdev_StoreHits',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/WTCHdev/WTCHdev_procdailyresults/proc/WTCHdev_StoreHits.sh ',
    dag=dag,
    email_on_failure=True,
    doc_md="""
    **WTCHdev Store Hits**
    
    **Purpose:**
    - Store development watch hits
    - Original CA job: tcWTCHdev_StoreHits
    - Original condition: s(tcWTCHwrd_WatchHitComFile_TRM)
    - Application: WTCHdev
    """
)

# ====== WTCHWRD REGIONAL PROCESSING ======
# Multiple regional processing boxes that depend on various conditions

# tbWTCHwrd_SetOrdToP_UP_OG_Daily - Original condition: s(tcWTCHwrd_WatchHitComFile_TRM)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_SetOrdToP_UP_OG_Daily', dag=dag) as up_og_daily_group:
    
    # tcWTCHwrd_SetOrdToP_UP_Daily - condition: n(tcWTCHwrd_SetOrdToP_UP)
    wtchwrd_setord_up_daily = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_UP_Daily',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd UP',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Set Orders to Process - UP Daily**
        
        **Purpose:**
        - Set status orders to process for UP region (daily)
        - Original CA job: tcWTCHwrd_SetOrdToP_UP_Daily
        - Original condition: n(tcWTCHwrd_SetOrdToP_UP)
        - Owner: test@INT
        """
    )
    
    # tcWTCHwrd_SetOrdToP_OG_Daily - condition: s(tcWTCHwrd_SetOrdToP_UP_Daily) & n(tcWTCHwrd_SetOrdToP_OG)
    wtchwrd_setord_og_daily = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_OG_Daily',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd OG',
        dag=dag,
        email_on_failure=True,
        doc_md="""
        **WTCHwrd Set Orders to Process - OG Daily**
        
        **Purpose:**
        - Set status orders to process for OG region (daily)
        - Original CA job: tcWTCHwrd_SetOrdToP_OG_Daily
        - Original condition: s(tcWTCHwrd_SetOrdToP_UP_Daily) & n(tcWTCHwrd_SetOrdToP_OG)
        """
    )
    
    # Dependencies within UP/OG daily group
    wtchwrd_setord_up_daily >> wtchwrd_setord_og_daily

# tbWTCHwrd_AW - Original condition: s(tcWTCHwrd_WatchHitComFile_TRM)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_AW', dag=dag) as wtchwrd_aw_group:
    
    # tcWTCHwrd_SetOrdToP_AW
    wtchwrd_setord_aw = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_AW',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd AW',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Set Orders to Process - AW Region**"""
    )
    
    # tcWTCHwrd_ProductionRun_AWc
    wtchwrd_production_aw = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_AWc',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_ProductionRunEva.cmd AWc',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Production Run - AW Region**"""
    )

    wtchwrd_setord_aw >> wtchwrd_production_aw

# tbWTCHdev - Original condition: s(tcWTCHdev_StoreHits)
with TaskGroup(group_id=f'{env_pre}bWTCHdev', dag=dag) as wtchdev_group:
    
    # tcWTCHdev_SetOrdToP_DW
    wtchdev_setord_dw = WinRMOperator(
        task_id=f'{env_pre}cWTCHdev_SetOrdToP_DW',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd DW',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHdev Set Orders to Process - DW**"""
    )
    
    # tcWTCHdev_ProductionRun_DW
    wtchdev_production_dw = WinRMOperator(
        task_id=f'{env_pre}cWTCHdev_ProductionRun_DW',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_ProductionRun.cmd DWc',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHdev Production Run - DW**"""
    )
    
    # tcWTCHdev_PrepareDevTrm
    wtchdev_prepare_dev_trm = SSHOperator(
        task_id=f'{env_pre}cWTCHdev_PrepareDevTrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/WTCHdev/WTCHdev_prepdevtrm/proc/WTCHdev_PrepareDevTrm.sh ',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHdev Prepare Device TRM - Batch Processing**"""
    )
    
    # Dependencies within WTCHdev group
    wtchdev_setord_dw >> wtchdev_production_dw >> wtchdev_prepare_dev_trm

# tbWTCHwrd_WW - Original condition: s(tcWTCHdev_StoreHits)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_WW', dag=dag) as wtchwrd_ww_group:
    
    # tcWTCHwrd_SetOrdToP_WW
    wtchwrd_setord_ww = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_WW',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd WW',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Set Orders to Process - WW Region**"""
    )
    
    # tcWTCHwrd_ProductionRun_WWc
    wtchwrd_production_ww = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_WWc',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_ProductionRunEva.cmd WWc',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Production Run - WW Region**"""
    )
    
    wtchwrd_setord_ww >> wtchwrd_production_ww

# tbWTCHwrd_TT - Original condition: s(tcWTCHdev_StoreHits)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_TT', dag=dag) as wtchwrd_tt_group:
    
    # tcWTCHwrd_SetOrdToP_TT
    wtchwrd_setord_tt = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_TT',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd TT',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Set Orders to Process - TT Region**"""
    )
    
    # tcWTCHwrd_ProductionRun_TTc
    wtchwrd_production_tt = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_TTc',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_ProductionRunEva.cmd TTc',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Production Run - TT Region**"""
    )
    
    wtchwrd_setord_tt >> wtchwrd_production_tt

# tbWTCHwrd_WWL - Original condition: s(tcWTCHdev_StoreHits)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_WWL', dag=dag) as wtchwrd_wwl_group:
    
    # tcWTCHwrd_SetOrdToP_WWL
    wtchwrd_setord_wwl = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_WWL',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd WWLc',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Set Orders to Process - WWL Region**"""
    )
    
    # tcWTCHwrd_ProductionRun_WWLc
    wtchwrd_production_wwl = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_ProductionRun_WWLc',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_ProductionRunEva.cmd WWLc',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Production Run - WWL Region**"""
    )
    
    wtchwrd_setord_wwl >> wtchwrd_production_wwl

# tbWTCHwrd_SetOrd2P - Manual Watch Processing - Original condition: s(tbWTCHwrd_WW)
with TaskGroup(group_id=f'{env_pre}bWTCHwrd_SetOrd2P', dag=dag) as wtchwrd_manual_group:
    
    # tcWTCHwrd_SetOrdToP_MAN
    wtchwrd_setord_man = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_MAN',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd MAN',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Set Orders to Process - Manual (MAN)**"""
    )
    
    # tcWTCHwrd_SetOrdToP_CN_MAN
    wtchwrd_setord_cn_man = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_CN_MAN',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd CN',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Set Orders to Process - CN Manual**"""
    )
    
    # tcWTCHwrd_SetOrdToP_ASSO
    wtchwrd_setord_asso = WinRMOperator(
        task_id=f'{env_pre}cWTCHwrd_SetOrdToP_ASSO',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\OPSi\proc\WTCHwrd_SetOrdToP.cmd ASSO',
        dag=dag,
        email_on_failure=True,
        doc_md="""**WTCHwrd Set Orders to Process - ASSO Manual**"""
    )
    
    # All manual tasks run in parallel (no dependencies specified in original JIL)
    [wtchwrd_setord_man, wtchwrd_setord_cn_man, wtchwrd_setord_asso]

# ====== MAIN WORKFLOW DEPENDENCIES ======
# Based on the complex dependency structure from the JIL file:

# 0. Initial trigger - ATRIUM MvComfile starts the entire workflow
atrium_mvcomfile >> comrec_group

# 1. COMrec processing triggers multiple downstream processes
# Original conditions: s(tbCOMrec)
comrec_group >> [atrium_qlty_comrec, atrium_ke3tool, wtchwrd_watchhitcomfile_trm, ctr_loader_group]

# 2. WTCHwrd_WatchHitComFile_TRM triggers multiple regional processing
wtchwrd_watchhitcomfile_trm >> [up_og_daily_group, wtchwrd_aw_group, wtchdev_storehits]

# 3. tcWTCHdev_StoreHits triggers multiple regional WTCHwrd processing
wtchdev_storehits >> [wtchdev_group, wtchwrd_ww_group, wtchwrd_tt_group, wtchwrd_wwl_group]

# 4. Manual processing depends on WW completion
wtchwrd_ww_group >> wtchwrd_manual_group