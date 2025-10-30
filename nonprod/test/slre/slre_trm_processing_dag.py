from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Param
import logging
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

# Import shared utilities
from utils.common_utils import (
    get_environment_from_path, 
    SSHConnections,
    check_file_exists,
    check_file_pattern
)

# Get environment from current DAG path
ENV = get_environment_from_path(__file__)
env = ENV.lower()
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
    f'{app_name}_trm_processing_{env}',
    default_args=default_args,
    description='SLRE TRM Processing Pipeline',
    schedule=None,  # Manual trigger or external dependency
    catchup=False,
    tags=[env, app_name, 'dataproc', 'trm', 'sensors'],
    params={
        "year": Param(type="string", description="Processing year (YYYY)", pattern="^\d{4}$"),
        "issue": Param(type="string", description="Month and day (NNYY)", pattern="^\d{4}$"),
        "pubdate": Param(type="string", description="Full date (YYYYMMDD)", pattern="^\d{8}$"),
        "limit": Param(6000,type="integer", description="Limit for processing")
    }
  
)

# SSH Connection IDs (using shared constants)
SSH_CONN_ID_1 = SSHConnections.TGEN_VL101  # Main processing server
SSH_CONN_ID_2 = SSHConnections.TGEN_VL105  # File monitoring server

# SLRE-specific file paths (dynamic based on environment)
SLRE_VCD_BUSY_FILE = f"/{ENV}/SHR/SLRE/work/SLRE_VCD.busy"
SLRE_AUTOIDX_FILE = f"/{ENV}/SHR/SLRE/work/autoidx"
SLRE_BATCHPROC_PATTERN = f"/{ENV}/SHR/SLRE/work/batchproc*"

# Preparation task for TRM processing
slre_preptrm = SSHOperator(
    task_id='tcSLRE_preptrm',
    ssh_conn_id=SSH_CONN_ID_1,
    command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_preptrm.sh {{ params.year }} {{ params.issue }} {{ params.pubdate }}',
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
        command=f'/{ENV}/LIB/SLRE/SLRE_cnvtrm/proc/SLRE_cnvtrm.sh {{params.limit}}',
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
        command=f'/{ENV}/LIB/SLRE/SLRE_mrgtrm/proc/SLRE_mrgtrm.sh ',
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
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_checkbpfiles.sh ',
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

# File sensors for pending processes (triggered after TRM completion) - Using simple SSH commands
sensor_pend_file = SSHOperator(
    task_id='tfSLRE_pend_sensor',
    ssh_conn_id=SSH_CONN_ID_2,
    command=check_file_exists(SLRE_AUTOIDX_FILE),
    dag=dag,
    email_on_failure=True,  # alarm_if_fail: 1
    doc_md="""
    **SLRE Pend File Sensor**
    
    **Purpose:**
    - Monitors for autoidx file creation
    - Triggers pending TRM processing workflow
    - Uses simple SSH test command for file existence
    """
)

sensor_book_file = SSHOperator(
    task_id='tfSLRE_book_sensor',
    ssh_conn_id=SSH_CONN_ID_2,
    command=check_file_pattern(SLRE_BATCHPROC_PATTERN),
    dag=dag,
    email_on_failure=True,  # alarm_if_fail: 1
    doc_md="""
    **SLRE Book File Sensor**
    
    **Purpose:**
    - Monitors for batchproc files creation
    - Triggers book processing workflow
    - Uses simple SSH pattern check for file existence
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
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_mailpend.sh ',
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
        command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_mailbook.sh ',
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
