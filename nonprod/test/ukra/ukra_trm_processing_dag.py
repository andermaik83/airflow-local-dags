from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
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
SSH_CONN_ID = resolve_connection_id(ENV, "opr_vl101")
WINRM_CONN_ID = resolve_connection_id(ENV, "opr_vw105")

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    f'{env_pre}d_{app_name}_trm',
    default_args=default_args,
    description='UKRA TRM Processing Pipeline - BOX tbUKRA_trm workflow',
    schedule=None,  # Manual trigger or external dependency
    catchup=False,
    tags=[env, app_name, 'dataproc', 'trm'],
)

# TaskGroup representing BOX tbUKRA_trm
with TaskGroup(group_id=f'{env_pre}bUKRA_trm', dag=dag) as trm_taskgroup:
    
    # tcUKRA_cnvtrm - Convert TRM files (first task, no dependencies within box)
    ukra_cnvtrm = SSHOperator(
        task_id=f'{env_pre}cUKRA_cnvtrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_APIcnvtrm/proc/UKRA_APIcnvtrm.sh convert 1500 N',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **UKRA Convert TRM Task**
        
        **Purpose:**
        - Converts TRM files for processing
        - First step in TRM processing pipeline
        - Parameters: convert 1500 N
        """
    )
    
    # tcUKRA_trancom - Translate output data conversion, depends on cnvtrm
    ukra_trancom = SSHOperator(
        task_id=f'{env_pre}cUKRA_trancom',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_APItrancom/proc/UKRA_APItrancom.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **UKRA Translate Common Task**
        
        **Purpose:**
        - Translates output data conversion
        - Processes converted TRM data
        """
    )
    
    # tcUKRA_checkforimg - Check for images, depends on cnvtrm
    ukra_checkforimg = SSHOperator(
        task_id=f'{env_pre}cUKRA_checkforimg',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_oper/proc/UKRA_Chckforimgs.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **UKRA Check for Images Task**
        
        **Purpose:**
        - Checks for images in the processing pipeline
        - Validates image availability
        """
    )
    
    # tcUKRA_mailmissingfields - Mail missing fields alert, depends on cnvtrm
    ukra_mailmissingfields = SSHOperator(
        task_id=f'{env_pre}cUKRA_mailmissingfields',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_oper/proc/UKRA_mailmissingfields.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **UKRA Mail Missing Fields Task**
        
        **Purpose:**
        - Sends notification about missing fields in conversion
        - Quality check and alert mechanism
        """
    )
    
    # tcUKRA_mailmissingtrm - Mail missing TRM alert, depends on cnvtrm
    ukra_mailmissingtrm = SSHOperator(
        task_id=f'{env_pre}cUKRA_mailmissingtrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_oper/proc/UKRA_mailmissingtrm.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **UKRA Mail Missing TRM Task**
        
        **Purpose:**
        - Sends notification about missing TRM files
        - Quality check and alert mechanism
        """
    )
    
    # tcUKRA_checkforprevBPfiles - Check for previous BP files, depends on trancom
    ukra_checkforprevbpfiles = SSHOperator(
        task_id=f'{env_pre}cUKRA_checkforprevBPfiles',
        ssh_conn_id=WINRM_CONN_ID,
        command=r'E:\local\tipsocr\proc\TIPSi_start_Check_BPDB_UKRA1.cmd',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **UKRA Check for Previous BP Files Task**
        
        **Purpose:**
        - Checks for previous batch processing files
        - Validates data readiness for merge
        - Runs on Windows machine (opr_vw103)
        """
    )
    
    # tcUKRA_mrgtrm - Merge TRM files, depends on checkforprevBPfiles
    ukra_mrgtrm = SSHOperator(
        task_id=f'{env_pre}cUKRA_mrgtrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_mrgtrm/proc/UKRA_mrgtrm.sh 1500',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **UKRA Merge TRM Task**
        
        **Purpose:**
        - Merges TRM files for processing
        - Consolidates data after BP file check
        - Parameter: 1500
        """
    )
    
    # tcUKRA_mv2bptrm - Move to batch processing, depends on mrgtrm
    ukra_mv2bptrm = SSHOperator(
        task_id=f'{env_pre}cUKRA_mv2bptrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_oper/proc/UKRA_mv2bptrm.sh ',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **UKRA Move to BP TRM Task**
        
        **Purpose:**
        - Moves merged files to batch processing directory
        - Prepares files for mail notification
        """
    )
    
    # tcUKRA_mailtrm - Mail TRM notification, depends on mv2bptrm
    ukra_mailtrm = SSHOperator(
        task_id=f'{env_pre}cUKRA_mailtrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_oper/proc/UKRA_mailtrm.sh ',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **UKRA Mail TRM Task**
        
        **Purpose:**
        - Sends notification email for TRM processing
        - Alerts operators about completed processing
        """
    )
    
    # tcUKRA_cleantrm - Cleanup TRM files, depends on mailtrm
    ukra_cleantrm = SSHOperator(
        task_id=f'{env_pre}cUKRA_cleantrm',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_oper/proc/UKRA_cleantrm.sh ',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **UKRA Clean TRM Task**
        
        **Purpose:**
        - Cleans up TRM processing files
        - Final cleanup step in TRM workflow
        """
    )
    
    # Define dependencies within TRM TaskGroup
    # cnvtrm is the starting point, branches out to multiple parallel tasks
    ukra_cnvtrm >> [ukra_trancom, ukra_checkforimg, ukra_mailmissingfields, ukra_mailmissingtrm]
    
    # trancom triggers checkforprevBPfiles
    ukra_trancom >> ukra_checkforprevbpfiles
    
    # Linear dependency chain for merge and cleanup
    ukra_checkforprevbpfiles >> ukra_mrgtrm >> ukra_mv2bptrm >> ukra_mailtrm >> ukra_cleantrm
