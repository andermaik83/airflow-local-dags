from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

# Import shared utilities
from utils.common_utils import (
    get_environment_from_path, 
    check_file,
    SSHConnections
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
    f'{app_name}_conversion_{env}',
    default_args=default_args,
    description='Image processing and monitoring',
    schedule=None, # Manual trigger or sensor-based
    catchup=False,
    tags=[env, app_name,'dataproc','conversion'],
)

# SSH Connection IDs (using constants from shared utilities)
SSH_CONN_ID_1 = SSHConnections.TGEN_VL101  # For tgen-vl101 server
SSH_CONN_ID_2 = SSHConnections.TGEN_VL105  # For tgen-vl105 server

# File paths for monitoring
ALBA_IMG_ISSUE_FILE = f"/{ENV}/SHR/ALBA/work/ALBA_imgissue.par"
ALBA_XML_ISSUE_FILE = f"/{ENV}/SHR/ALBA/work/ALBA_xmlissue.par"
ALBA_NO_IMG_FILE = f"/{ENV}/SHR/ALBA/work/ALBA_noimgtoprocess"


# Task 1: Check issues (prerequisite for file watchers)
alba_checkissue = SSHOperator(
    task_id=f'alba_checkissue_{env}',
    ssh_conn_id=SSH_CONN_ID_2,
    command=f'/{ENV}/LIB/ALBA/ALBA_oper/proc/ALBA_chkissues.sh ',
    dag=dag,
    doc_md="""
    **ALBA Check Issues Task**
    
    - Runs check script to identify ALBA processing issues
    - Creates monitoring files for downstream file sensors
    - Wiki: https://wiki.clarivate.io/display/OPERS/ALBA+images
    """
)

# Task Group for File Watchers (equivalent to FT jobs) - Now using simple SSHOperator
with TaskGroup("alba_file_watchers", dag=dag) as file_watchers_group:
    
    # File watcher for images issue - using simple SSH command
    alba_check_images = SSHOperator(
        task_id=f'alba_check_images_{env}',
        ssh_conn_id=SSH_CONN_ID_2,
        command=check_file(ALBA_IMG_ISSUE_FILE),
        dag=dag,
        doc_md="""
        **ALBA Check Images File Watcher**
        
        - Monitors creation of ALBA_imgissue.par file
        - Triggers image processing workflow when file is created
        - Uses simple SSH test command for file existence
        """
    )
    
    # File watcher for XML issue - using simple SSH command
    alba_check_xml = SSHOperator(
        task_id=f'alba_check_xml_{env}',
        ssh_conn_id=SSH_CONN_ID_2,
        command=check_file(ALBA_XML_ISSUE_FILE),
        dag=dag,
        doc_md="""
        **ALBA Check XML File Watcher**
        
        - Monitors creation of ALBA_xmlissue.par file
        - Part of XML processing workflow
        - Uses simple SSH test command for file existence
        """
    )
    
    # File watcher for no images - using simple SSH command
    alba_check_noimages = SSHOperator(
        task_id=f'alba_check_noimages_{env}',
        ssh_conn_id=SSH_CONN_ID_2,
        command=check_file(ALBA_NO_IMG_FILE),
        dag=dag,
        doc_md="""
        **ALBA Check No Images File Watcher**
        
        - Monitors creation of ALBA_noimgtoprocess file
        - Indicates no images to process condition
        - Uses simple SSH test command for file existence
        """
    )

# Task Group for Image Processing (equivalent to BOX tbALBA_images)
with TaskGroup("alba_image_processing", dag=dag) as image_processing_group:
    
    # Step 1: Rename images
    alba_renimg = SSHOperator(
        task_id=f'alba_renimg_{env}',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/ALBA/ALBA_renimg/proc/ALBA_renimg.sh ',
        dag=dag,
        doc_md="""
        **ALBA Rename Images Task**
        
        - First step in image processing workflow
        - Renames image files as part of processing
        """
    )
    
    # Step 2: Convert images (depends on rename success)
    alba_cnvimg = SSHOperator(
        task_id=f'alba_cnvimg_{env}',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/ALBA/ALBA_cnvimg/proc/ALBA_cnvimg.sh ',
        dag=dag,
        doc_md="""
        **ALBA Convert Images Task**
        
        - Converts images after successful rename
        - Part of main image processing pipeline
        """
    )
    
    # Step 3: Convert loaded images (depends on convert success)
    alba_cnvldimg = SSHOperator(
        task_id=f'alba_cnvldimg_{env}',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/ALBA/ALBA_cnvldimg/proc/ALBA_cnvldimg.sh ',
        dag=dag,
        doc_md="""
        **ALBA Convert Loaded Images Task**
        
        - Final image conversion step
        - Processes loaded images
        """
    )
    
    # Error handling: Mail errors if rename fails
    alba_mail_errors_renimg = SSHOperator(
        task_id=f'alba_mail_errors_renimg_{env}',
        ssh_conn_id=SSH_CONN_ID_1,
        command=f'/{ENV}/LIB/ALBA/ALBA_oper/proc/ALBA_mail_errors_renimg.sh ',
        trigger_rule=TriggerRule.ONE_FAILED,  # Runs only if alba_renimg fails
        dag=dag,
        doc_md="""
        **ALBA Mail Rename Errors Task**
        
        - Sends error notifications if image rename fails
        - Conditional task (f(tcALBA_renimg) in Autosys)
        """
    )
    
    # Success event: Send success event
    alba_success_renimg = BashOperator(
        task_id=f'alba_success_renimg_{env}',
        bash_command='echo "SUCCESS: tcALBA_renimg completed successfully"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
        doc_md="""
        **ALBA Success Event Task**
        
        - Sends success event when error handling completes
        - Originally used sendevent command
        """
    )
    
    # Final step: Mail images summary
    alba_mail_images = SSHOperator(
        task_id=f'alba_mail_images_{env}',
        ssh_conn_id=SSH_CONN_ID_2,
        command=f'/{ENV}/LIB/ALBA/ALBA_oper/proc/ALBA_mail_images.sh ',
        dag=dag,
        doc_md="""
        **ALBA Mail Images Summary Task**
        
        - Sends summary email of image processing results
        - Final task in image processing workflow
        """
    )

# XML Processing Task (depends on both XML and no-images file watchers)
alba_mv_xml = SSHOperator(
    task_id=f'alba_mv_xml_{env}',
    ssh_conn_id=SSH_CONN_ID_2,
    command=f'/{ENV}/LIB/ALBA/ALBA_oper/proc/ALBA_mv_XML2IArcanum.sh ',
    doc_md="""
    **ALBA Move XML Task**
    
    - Moves XML files to IArcanum system
    - Requires both XML and no-images conditions to be met
    """
)

# Define task dependencies

# 1. Check issues must run first (prerequisite for file watchers)
alba_checkissue >> file_watchers_group

# 2. Image processing workflow dependencies
alba_check_images >> image_processing_group
alba_renimg >> alba_cnvimg >> alba_cnvldimg >> alba_mail_images

# 3. Error handling path
alba_renimg >> alba_mail_errors_renimg >> alba_success_renimg

# 4. XML processing depends on both XML and no-images checks
[alba_check_xml, alba_check_noimages] >> alba_mv_xml