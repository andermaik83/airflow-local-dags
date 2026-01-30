from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Param
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
SSH_CONN_ID = resolve_connection_id(ENV, "gen_vl101")

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    f'{env_pre}d_{app_name}_cnvldimg',
    default_args=default_args,
    description='UKRA Convert and Load Images Pipeline',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=[env, app_name, 'dataproc', 'images', 'load'],
    params={
        "atrium_num": Param(
            type="integer", 
            description="Atrium number for conversion and loading (required)",
            minimum=1
        )
    }
)

# tcUKRA_cnvldimg - Convert and load images
ukra_cnvldimg = SSHOperator(
    task_id=f'{env_pre}cUKRA_cnvldimg',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/UKRA/UKRA_cnvldimg/proc/UKRA_cnvldimg.sh {{{{ params.atrium_num }}}}',
    dag=dag,
    email_on_failure=False,  # alarm_if_fail: 0
    doc_md="""
    **UKRA Convert and Load Images Task**
    
    **Purpose:**
    - Converts and loads images for UKRA processing
    - Atrium number must be provided when triggering the DAG
    
    **Parameters:**
    - atrium_num: Atrium number for conversion (REQUIRED - must be specified in Airflow GUI)
    """
)

# tcUKRA_mail_images - Mail images notification, depends on cnvldimg
ukra_mail_images = SSHOperator(
    task_id=f'{env_pre}cUKRA_mail_images',
    ssh_conn_id=SSH_CONN_ID,
    command=f'/{ENV}/LIB/UKRA/UKRA_oper/proc/UKRA_mail_images.sh ',
    dag=dag,
    email_on_failure=False,  # alarm_if_fail: 0
    doc_md="""
    **UKRA Mail Images Task**
    
    **Purpose:**
    - Sends notification email for image conversion completion
    - Alerts operators about successful image loading
    - Final step in cnvldimg workflow
    """
)

# Define dependencies
ukra_cnvldimg >> ukra_mail_images
