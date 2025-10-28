from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import logging
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

# Import shared utilities
from utils.common_utils import get_environment_from_path, SSHConnections, check_file_exists

# Get environment from current DAG path
ENV = get_environment_from_path(__file__)
env = ENV.lower()
app_name = os.path.basename(os.path.dirname(__file__))

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # alarm_if_fail: 1 for viennacodes tasks
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'{app_name}_viennacodes_{env}',
    default_args=default_args,
    description=f'SLRE Vienna Codes Processing Pipeline - {ENV}',
    schedule='*/5 * * * *',  # Check every 5 minutes for file
    catchup=False,
    max_active_runs=1,  # Prevent multiple concurrent runs
    tags=[env, app_name, 'dataproc', 'viennacodes', 'file-sensor'],
)

# SSH Connection IDs (using shared constants)
SSH_CONN_ID_1 = "tgen-vl101"  # Linux processing server
SSH_CONN_ID_3 = "topr_vw103"  # Windows server for batch processing

# SLRE VCD file path for monitoring
SLRE_VCD_BUSY_FILE = f"/{ENV}/SHR/SLRE/work/SLRE_VCD.busy"

# File sensor function for VCD start file
def check_vcd_start_file(**context):
    """Check for VCD start file - tfSLRE_start_VCD equivalent"""
    return check_file_exists(SLRE_VCD_BUSY_FILE, SSH_CONN_ID_1)

# File sensor task - tfSLRE_start_VCD_sensor
tfSLRE_start_VCD_sensor = PythonOperator(
    task_id='tfSLRE_start_VCD_sensor',
    python_callable=check_vcd_start_file,
    dag=dag,
    email_on_failure=False,  # alarm_if_fail: 0
    doc_md=f"""
    **SLRE VCD Start File Sensor - {ENV}**
    
    **Purpose:**
    - Monitors for VCD input files
    - Triggers Vienna codes processing workflow when file exists
    - File monitored: {SLRE_VCD_BUSY_FILE}
    - Environment: {ENV}
    
    **Behavior:**
    - Checks for file existence on each DAG run
    - Automatically triggers tbSLRE_viennacodes when file is found
    """
)

# TaskGroup representing BOX tbSLRE_viennacodes
with TaskGroup(group_id='tbSLRE_viennacodes', dag=dag) as viennacodes_taskgroup:
    
    # tcSLRE_cnvviennacodexml - First task in the BOX
    slre_cnvviennacodexml = SSHOperator(
        task_id='tcSLRE_cnvviennacodexml',
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
        task_id='tcSLRE_mv2bpvc',
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
        task_id='tcSLRE_autobp_VC',
        ssh_conn_id=SSH_CONN_ID_3,  # Windows server
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
        task_id='tcSLRE_cleanup_vc',
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
