from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import logging
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

# Import shared utilities
from utils import check_file_exists, check_file_exists_with_pattern, get_environment_from_path

# Get environment from current DAG path
env_lower = get_environment_from_path(__file__)
ENV = env_lower.upper()
app_name = os.path.basename(os.path.dirname(__file__))

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'{app_name}_trm_processing_{env_lower}',
    default_args=default_args,
    description='SLRE TRM Processing Pipeline',
    schedule=None,  # Manual trigger or external dependency
    catchup=False,
    tags=[env_lower, app_name, 'dataproc', 'trm', 'sensors'],
)

# SSH Connection IDs (using shared constants)
SSH_CONN_ID_1 = SSHConnections.TGEN_VL101  # Main processing server
SSH_CONN_ID_2 = SSHConnections.TGEN_VL105  # File monitoring server

# SLRE-specific file paths (dynamic based on environment)
SLRE_VCD_BUSY_FILE = f"/{ENV}/SHR/SLRE/work/SLRE_VCD.busy"
SLRE_AUTOIDX_FILE = f"/{ENV}/SHR/SLRE/work/autoidx"
SLRE_BATCHPROC_PATTERN = f"/{ENV}/SHR/SLRE/work/batchproc*"

# File sensor functions using shared utilities
def check_vcd_file(**context):
    """Check for VCD start file - tfSLRE_start_VCD equivalent"""
    return check_file_exists(SLRE_VCD_BUSY_FILE, SSH_CONN_ID_2)

def check_pend_file(**context):
    """Check for autoidx file - tfSLRE_pend equivalent"""
    return check_file_exists(SLRE_AUTOIDX_FILE, SSH_CONN_ID_2)

def check_book_file(**context):
    """Check for batchproc file - tfSLRE_book equivalent"""
    return check_file_exists_with_pattern(SLRE_BATCHPROC_PATTERN, SSH_CONN_ID_2)

# File sensor tasks replacing FT jobs
sensor_vcd_start = PythonOperator(
    task_id='tfSLRE_start_VCD_sensor',
    python_callable=check_vcd_file,
    dag=dag,
    email_on_failure=False,  # alarm_if_fail: 0
    doc_md="""
    **SLRE VCD Start File Sensor**
    
    **Purpose:**
    - Monitors for VCD input files
    - Triggers Vienna codes processing workflow    
    """
)

# Preparation task for TRM processing
slre_preptrm = SSHOperator(
    task_id='tcSLRE_preptrm',
    ssh_conn_id=SSH_CONN_ID_1,
    command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_preptrm.sh 2022 0913 20220913',
    dag=dag,
    email_on_failure=False,  # alarm_if_fail: 0
    doc_md="""
    **SLRE Prepare TRM Task**
    
    **Purpose:**
    - Prepares TRM processing environment
    - Sets up data for TRM workflow
    """
)

# TaskGroup representing BOX tbSLRE_trm
with TaskGroup(group_id='tbSLRE_trm', dag=dag) as trm_taskgroup:
    
    # tcSLRE_cnvtrm - Convert TRM files
    slre_cnvtrm = SSHOperator(
        task_id='tcSLRE_cnvtrm',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_cnvtrm/proc/SLRE_cnvtrm.sh 6000',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **SLRE Convert TRM Task**
        
        **Purpose:**
        - Converts TRM files for processing
        - First step in TRM processing pipeline
        """
    )
    
    # tcSLRE_mrgtrm - Merge TRM files, depends on cnvtrm
    slre_mrgtrm = SSHOperator(
        task_id='tcSLRE_mrgtrm',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_mrgtrm/proc/SLRE_mrgtrm.sh',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **SLRE Merge TRM Task**
        
        **Purpose:**
        - Merges converted TRM files
        - Consolidates TRM data for checking
        """
    )
    
    # tcSLRE_check_bpfiles - Check batch processing files, depends on mrgtrm
    slre_check_bpfiles = SSHOperator(
        task_id='tcSLRE_check_bpfiles',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_checkbpfiles.sh',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Check BP Files Task**
        
        **Purpose:**
        - Validates batch processing files
        - Ensures TRM files are ready for processing
        """
    )
    
    # Define dependencies within TRM TaskGroup
    slre_cnvtrm >> slre_mrgtrm >> slre_check_bpfiles

# File sensors for pending processes (triggered after TRM completion)
sensor_pend_file = PythonOperator(
    task_id='tfSLRE_pend_sensor',
    python_callable=check_pend_file,
    dag=dag,
    email_on_failure=True,  # alarm_if_fail: 1
    doc_md="""
    **SLRE Pend File Sensor**
    
    **Purpose:**
    - Monitors for autoidx file creation
    - Triggers pending TRM processing workflow
    """
)

sensor_book_file = PythonOperator(
    task_id='tfSLRE_book_sensor',
    python_callable=check_book_file,
    dag=dag,
    email_on_failure=True,  # alarm_if_fail: 1
    doc_md="""
    **SLRE Book File Sensor**
    
    **Purpose:**
    - Monitors for batchproc files creation
    - Triggers book processing workflow
    """
)

# TaskGroup representing BOX tbSLRE_trmpend (triggered by pend sensor)
with TaskGroup(group_id='tbSLRE_trmpend', dag=dag) as trmpend_taskgroup:
    
    # tcSLRE_autoidxtrm - Auto index TRM
    slre_autoidxtrm = SSHOperator(
        task_id='tcSLRE_autoidxtrm',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/TIPSi/TIPSi_indexing/proc/TIPSi_indexing.sh SLRE output_mrgtrm output_autoidx',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **SLRE Auto Index TRM Task**
        
        **Purpose:**
        - Automatically indexes TRM data
        - Creates searchable index for TRM content
        """
    )
    
    # tcSLRE_move2bptrm_autoidx - Move to batch processing autoidx
    slre_move2bptrm_autoidx = SSHOperator(
        task_id='tcSLRE_move2bptrm_autoidx',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_mv2bptrm.sh autoidx',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **SLRE Move to BP TRM Autoidx Task**
        
        **Purpose:**
        - Moves autoidx files to batch processing
        - Prepares files for mail notification
        """
    )
    
    # tcSLRE_mailtrmpend - Mail TRM pending notification
    slre_mailtrmpend = SSHOperator(
        task_id='tcSLRE_mailtrmpend',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_mailpend.sh',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **SLRE Mail TRM Pending Task**
        
        **Purpose:**
        - Sends notification email for pending TRM
        - Alerts operators about completed processing
        """
    )
    
    # Dependencies within trmpend TaskGroup
    slre_autoidxtrm >> slre_move2bptrm_autoidx >> slre_mailtrmpend

# TaskGroup representing BOX tbSLRE_trmbook (triggered by book sensor)
with TaskGroup(group_id='tbSLRE_trmbook', dag=dag) as trmbook_taskgroup:
    
    # tcSLRE_move2bptrm_bp - Move to batch processing batchproc
    slre_move2bptrm_bp = SSHOperator(
        task_id='tcSLRE_move2bptrm_bp',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_mv2bptrm.sh batchproc',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **SLRE Move to BP TRM Batchproc Task**
        
        **Purpose:**
        - Moves batchproc files to processing directory
        - Prepares files for book notification
        """
    )
    
    # tcSLRE_mailtrmbook - Mail TRM book notification
    slre_mailtrmbook = SSHOperator(
        task_id='tcSLRE_mailtrmbook',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_mailbook.sh',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **SLRE Mail TRM Book Task**
        
        **Purpose:**
        - Sends notification email for book processing
        - Alerts operators about completed book workflow
        """
    )
    
    # Dependencies within trmbook TaskGroup
    slre_move2bptrm_bp >> slre_mailtrmbook

# Define main workflow dependencies
# Preparation triggers TRM processing
slre_preptrm >> trm_taskgroup

# TRM completion triggers file sensors
trm_taskgroup >> [sensor_pend_file, sensor_book_file]

# File sensors trigger their respective processing groups
sensor_pend_file >> trmpend_taskgroup
sensor_book_file >> trmbook_taskgroup
