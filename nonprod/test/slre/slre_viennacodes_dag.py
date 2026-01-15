from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.standard.sensors.filesystem import FileSensor
import logging
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

# Import shared utilities
from utils.common_utils import get_environment_from_path, resolve_connection_id

# Get environment from current DAG path
ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]
app_name = os.path.basename(os.path.dirname(__file__))

# SSH Connection IDs
SSH_CONN_ID_1 = resolve_connection_id(ENV, "opr_vl101")
SSH_CONN_ID_2 = resolve_connection_id(ENV, "opr_vw105")

# SLRE VCD file path for monitoring
SLRE_VCD_BUSY_FILE = f"/{ENV}/SHR/SLRE/work/SLRE_VCD.busy"

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # alarm_if_fail: 1 for viennacodes tasks
    'email_on_retry': False
}

dag = DAG(
    f'{env_pre}d_{app_name}_viennacodes',
    default_args=default_args,
    description=f'SLRE Vienna Codes Processing Pipeline - {ENV}',
    schedule='*/5 * * * *',
    catchup=False,
    max_active_runs=1,  # Prevent multiple concurrent runs
    tags=[env, app_name, 'dataproc', 'viennacodes', 'file-sensor'],
)

# File sensor task - tfSLRE_start_VCD_sensor (using simple SSH command)
tfSLRE_start_VCD_sensor = FileSensor(
    task_id=f'{env_pre}fSLRE_start_VCD_sensor',
    filepath=SLRE_VCD_BUSY_FILE,
    fs_conn_id=SSH_CONN_ID_1,
    poke_interval=30,  # Check every 30 seconds
    timeout=240,  # 4 minute timeout (less than 5-minute schedule)
    mode='poke',
    dag=dag,
    doc_md=f"""
    **SLRE VCD Start File Sensor - {ENV}**
    
    **Purpose:**
    - Monitors for VCD input files every 5 minutes
    - Processes files when found, skips when not found
    - File monitored: {SLRE_VCD_BUSY_FILE}
    """
)

# TaskGroup representing BOX tbSLRE_viennacodes
with TaskGroup(group_id=f'{env_pre}bSLRE_viennacodes', dag=dag) as viennacodes_taskgroup:
    
    # tcSLRE_cnvviennacodexml - First task in the BOX
    slre_cnvviennacodexml = SSHOperator(
        task_id=f'{env_pre}cSLRE_cnvviennacodexml',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_cnvviennacodexml/proc/SLRE_cnvviennacodexml.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Convert Vienna Code XML Task**
        
        **Purpose:**
        - Converts Vienna code data to XML format
        - First step in Vienna codes processing pipeline
        """
    )
    
    # tcSLRE_mv2bpvc - Move to batch processing, depends on cnvviennacodexml
    slre_mv2bpvc = SSHOperator(
        task_id=f'{env_pre}cSLRE_mv2bpvc',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_mv2bpvc.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Move to Batch Processing VC Task**
        
        **Purpose:**
        - Moves Vienna code files to batch processing directory
        - Prepares files for Windows batch processing
        """
    )
    
    # tcSLRE_autobp_VC - Auto batch processing on Windows, depends on mv2bpvc
    slre_autobp_vc = SSHOperator(
        task_id=f'{env_pre}cSLRE_autobp_VC',
        ssh_conn_id=SSH_CONN_ID_2,  # Windows server
        command='e:\\Local\\TIPSocr\\proc\\TIPSi_start_AutoBatchproc_VC.cmd SLRE',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Auto Batch Processing VC Task**
        
        **Purpose:**
        - Executes Vienna codes batch processing on Windows server
        - OCR and processing of Vienna code images
        """
    )
    
    # tcSLRE_cleanup_vc - Cleanup Vienna codes, depends on autobp_VC
    slre_cleanup_vc = SSHOperator(
        task_id=f'{env_pre}cSLRE_cleanup_vc',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_cleanupvc.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Cleanup Vienna Codes Task**
        
        **Purpose:**
        - Cleanup Vienna codes processing files
        - Final step in Vienna codes workflow
        """
    )
    
    # Define dependencies within the TaskGroup
    slre_cnvviennacodexml >> slre_mv2bpvc >> slre_autobp_vc >> slre_cleanup_vc

# Define main workflow dependencies
# File sensor triggers Vienna codes processing
tfSLRE_start_VCD_sensor >> viennacodes_taskgroup
