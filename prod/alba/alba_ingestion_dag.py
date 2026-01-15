from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

# Import shared utilities
from utils.common_utils import get_environment_from_path

# Get environment from current DAG path
ENV = get_environment_from_path(__file__)
env = ENV.lower()
app_name = os.path.basename(os.path.dirname(__file__))


# DAG Definition
default_args = {
    'owner': 'test', 
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # alarm_if_fail: 1 in Autosys
    'email_on_retry': False
}

dag = DAG(
    f'{app_name}_ingestion_dag_{env}',
    default_args=default_args,
    description='Download and prepare image data from Unumbio',
    schedule=None,  # Manual trigger or can be scheduled as needed
    catchup=False,
    tags=[env, app_name,'dataproc','ingestion','unumbio'],
)

# SSH Connection ID for tgen-vl105 server
SSH_CONN_ID = 'tgen_vl105'

# Task 1: Download image data from Unumbio
alba_download_imgdata = SSHOperator(
    task_id=f'alba_download_imgdata_{env}',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/ALBA/ALBA_oper/proc/ALBA_dld_img_data_from_unumbio.sh ',
    dag=dag,
    doc_md="""
    **ALBA Download Image Data Task**
       
    **Purpose:**
    - Initial step in ALBA ingestion pipeline
    - Retrieves image files and associated data from Unumbio source
    - Prerequisites for downstream image preparation tasks
    """
)

# Task 2: Prepare image data 
# Condition: success of download task
alba_prepare_imgdata = SSHOperator(
    task_id=f'alba_prepare_imgdata_{env}',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/ALBA/ALBA_oper/proc/ALBA_prepdataimg.sh ',
    dag=dag,
    doc_md="""
    **ALBA Prepare Image Data Task**
      
    **Purpose:**
    - Second step in ALBA ingestion pipeline
    - Processes and prepares downloaded image data
    - Formats data for downstream ALBA processing workflows
    
    **Dependencies:**
    - Requires successful completion of image download task
    """
)

# Define task dependencies
# Sequential execution: download -> prepare
alba_download_imgdata >> alba_prepare_imgdata
