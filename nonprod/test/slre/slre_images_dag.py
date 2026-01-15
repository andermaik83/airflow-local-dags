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

# SSH Connection
SSH_CONN_ID = resolve_connection_id(ENV, "opr_vl102")

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False, 
    'email_on_retry': False
}

dag = DAG(
    f'{env_pre}d_{app_name}_images',
    default_args=default_args,
    description='SLRE Images Processing Pipeline - BOX tbSLRE_images workflow',
    schedule=None,  # Manual trigger or external dependency
    catchup=False,
    tags=[env, app_name, 'dataproc', 'images'],
)

# TaskGroup representing BOX tbSLRE_images
with TaskGroup(group_id=f'{env_pre}bSLRE_images', dag=dag) as images_taskgroup:
    
    # tcSLRE_renimg - First task in the BOX
    slre_renimg = SSHOperator(
        task_id=f'{env_pre}cSLRE_renimg',
        ssh_conn_id=SSH_CONN_ID,
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
        task_id=f'{env_pre}cSLRE_grpimgs',
        ssh_conn_id=SSH_CONN_ID,
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
        task_id=f'{env_pre}cSLRE_cnvimg',
        ssh_conn_id=SSH_CONN_ID,
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
        task_id=f'{env_pre}cSLRE_ldimg',
        ssh_conn_id=SSH_CONN_ID,
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
