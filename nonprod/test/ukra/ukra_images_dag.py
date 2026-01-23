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
SSH_CONN_ID = resolve_connection_id(ENV, "opr_vl101")

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
    description='UKRA Images Processing Pipeline - BOX tbUKRA_images workflow',
    schedule=None,  # Manual trigger or external dependency
    catchup=False,
    tags=[env, app_name, 'dataproc', 'images'],
)

# TaskGroup representing BOX tbUKRA_images
with TaskGroup(group_id=f'{env_pre}bUKRA_images', dag=dag) as images_taskgroup:
    
    # tcUKRA_remove_startfile - Remove start file (first task in the BOX)
    ukra_remove_startfile = SSHOperator(
        task_id=f'{env_pre}cUKRA_remove_startfile',
        ssh_conn_id=SSH_CONN_ID,
        command=f'rm -f /{ENV}/SHR/UKRA/work/START_IMAGES',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **UKRA Remove Start File Task**
        
        **Purpose:**
        - Removes the START_IMAGES flag file
        - First step in UKRA images processing
        - Prepares environment for image conversion
        """
    )
    
    # tcUKRA_cnvimg - Convert images, depends on remove_startfile
    ukra_cnvimg = SSHOperator(
        task_id=f'{env_pre}cUKRA_cnvimg',
        ssh_conn_id=SSH_CONN_ID,
        command=f'/{ENV}/LIB/UKRA/UKRA_cnvimg/proc/UKRA_cnvimg.sh ',
        dag=dag,
        email_on_failure=False,  # alarm_if_fail: 0
        doc_md="""
        **UKRA Convert Images Task**
        
        **Purpose:**
        - Converts images to required format
        - Processes UKRA images for loading
        - Final step in images workflow
        """
    )
    
    # Define dependencies within images TaskGroup
    ukra_remove_startfile >> ukra_cnvimg
