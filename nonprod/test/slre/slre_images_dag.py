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
from utils.common_utils import get_environment_from_path

# Get environment from current DAG path
env_lower = get_environment_from_path(__file__)
ENV = env_lower.upper()
app_name = os.path.basename(os.path.dirname(__file__))

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'{app_name}_images_{env_lower}',
    default_args=default_args,
    description='SLRE Images Processing Pipeline - BOX tbSLRE_images workflow',
    schedule=None,  # Manual trigger or external dependency
    catchup=False,
    tags=[env_lower, app_name, 'dataproc', 'images'],
)

# SSH Connection IDs (using shared constants)
SSH_CONN_ID_1 = SSHConnections.TGEN_VL101  # Main processing server

# TaskGroup representing BOX tbSLRE_images
with TaskGroup(group_id='tbSLRE_images', dag=dag) as images_taskgroup:
    
    # tcSLRE_renimg - First task in the BOX
    slre_renimg = SSHOperator(
        task_id='tcSLRE_renimg',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_renimg/proc/SLRE_renimg.sh ',
        dag=dag,
        doc_md="""
        **SLRE Rename Images Task**
        
        **Purpose:**
        - First step in SLRE images processing
        - Renames and organizes image files for processing
        """
    )
    
    # tcSLRE_grpimgs - Groups images, depends on renimg
    slre_grpimgs = SSHOperator(
        task_id='tcSLRE_grpimgs',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_grpimgs/proc/SLRE_startgrpimgs.sh ',
        dag=dag,
        doc_md="""
        **SLRE Group Images Task**
        
        **Purpose:**
        - Groups and organizes renamed images
        - Prepares images for conversion processing
        """
    )
    
    # tcSLRE_cnvimg - Convert images, depends on grpimgs
    slre_cnvimg = SSHOperator(
        task_id='tcSLRE_cnvimg',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_cnvimg/proc/SLRE_cnvimg.sh ',
        dag=dag,
        doc_md="""
        **SLRE Convert Images Task**
        
        **Purpose:**
        - Converts images to required format
        - Processes grouped images for loading        
        """
    )
    
    # tcSLRE_ldimg - Load images, depends on cnvimg
    slre_ldimg = SSHOperator(
        task_id='tcSLRE_ldimg',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/SLRE/SLRE_cnvldimg/proc/SLRE_cnvldimg.sh ',
        dag=dag,
        doc_md="""
        **SLRE Load Images Task**
        
        **Purpose:**
        - Final step in image processing pipeline
        - Loads converted images into target system
        """
    )
    
    # Define dependencies within the TaskGroup
    slre_renimg >> slre_grpimgs >> slre_cnvimg >> slre_ldimg

# Standalone cleanup task - scheduled separately (Sunday 19:00)
slre_cleanup_shrdir = SSHOperator(
    task_id='tcSLRE_cleanup_SHRDIR',
    ssh_conn_id=SSH_CONN_ID_1,
    command=f'/{ENV}/LIB/SLRE/SLRE_oper/proc/SLRE_cleanup_SHRDIR.sh 490',
    dag=dag,
    doc_md="""
    **SLRE Cleanup Shared Directory Task**
    
    **Purpose:**
    - Cleanup shared directory maintenance
    - Scheduled for Sunday 19:00
    """
)
