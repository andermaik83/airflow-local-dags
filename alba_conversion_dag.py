from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import logging

# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'alba_conversion_dag',
    default_args=default_args,
    description='Image processing and monitoring',
    schedule=None, # Manual trigger or sensor-based
    catchup=False,
    tags=['test','alba','datdproc'],
)

# SSH Connection IDs
SSH_CONN_ID_1 = 'tgen_vl101'  # For tgen-vl101 server
SSH_CONN_ID_2 = 'tgen_vl105'  # For tgen-vl105 server

# File paths for monitoring
ALBA_IMG_ISSUE_FILE = "/TEST/SHR/ALBA/work/ALBA_imgissue.par"
ALBA_XML_ISSUE_FILE = "/TEST/SHR/ALBA/work/ALBA_xmlissue.par"
ALBA_NO_IMG_FILE = "/TEST/SHR/ALBA/work/ALBA_noimgtoprocess"

def check_file_exists(**context):
    """Check if file exists on remote server"""
    ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID_2)
    file_path = context['params']['file_path']
    
    try:
        # Check if file exists
        stdin, stdout, stderr = ssh_hook.get_conn().exec_command(f'test -f {file_path} && echo "EXISTS" || echo "NOT_EXISTS"')
        result = stdout.read().decode().strip()
        
        if result == "EXISTS":
            logging.info(f"File {file_path} exists")
            return True
        else:
            logging.info(f"File {file_path} does not exist")
            return False
    except Exception as e:
        logging.error(f"Error checking file {file_path}: {str(e)}")
        return False

# Task 1: Check issues (prerequisite for file watchers)
alba_checkissue = SSHOperator(
    task_id='alba_checkissue',
    ssh_conn_id=SSH_CONN_ID_2,
    command='/TEST/LIB/ALBA/ALBA_oper/proc/ALBA_chkissues.sh ',
    dag=dag,
    doc_md="""
    **ALBA Check Issues Task**
    
    - Runs check script to identify ALBA processing issues
    - Creates monitoring files for downstream file sensors
    - Wiki: https://wiki.clarivate.io/display/OPERS/ALBA+images
    """
)

# Task Group for File Watchers (equivalent to FT jobs)
with TaskGroup("alba_file_watchers", dag=dag) as file_watchers_group:
    
    # File watcher for images issue
    alba_check_images = PythonOperator(
        task_id='alba_check_images',
        python_callable=check_file_exists,
        params={'file_path': ALBA_IMG_ISSUE_FILE},
        dag=dag,
        doc_md="""
        **ALBA Check Images File Watcher**
        
        - Monitors creation of ALBA_imgissue.par file
        - Triggers image processing workflow when file is created
        """
    )
    
    # File watcher for XML issue
    alba_check_xml = PythonOperator(
        task_id='alba_check_xml',
        python_callable=check_file_exists,
        params={'file_path': ALBA_XML_ISSUE_FILE},
        dag=dag,
        doc_md="""
        **ALBA Check XML File Watcher**
        
        - Monitors creation of ALBA_xmlissue.par file
        - Part of XML processing workflow
        """
    )
    
    # File watcher for no images
    alba_check_noimages = PythonOperator(
        task_id='alba_check_noimages',
        python_callable=check_file_exists,
        params={'file_path': ALBA_NO_IMG_FILE},
        dag=dag,
        doc_md="""
        **ALBA Check No Images File Watcher**
        
        - Monitors creation of ALBA_noimgtoprocess file
        - Indicates no images to process condition
        """
    )

# Task Group for Image Processing (equivalent to BOX tbALBA_images)
with TaskGroup("alba_image_processing", dag=dag) as image_processing_group:
    
    # Step 1: Rename images
    alba_renimg = SSHOperator(
        task_id='alba_renimg',
        ssh_conn_id=SSH_CONN_ID_1,
        command='/TEST/LIB/ALBA/ALBA_renimg/proc/ALBA_renimg.sh ',
        dag=dag,
        doc_md="""
        **ALBA Rename Images Task**
        
        - First step in image processing workflow
        - Renames image files as part of processing
        """
    )
    
    # Step 2: Convert images (depends on rename success)
    alba_cnvimg = SSHOperator(
        task_id='alba_cnvimg',
        ssh_conn_id=SSH_CONN_ID_1,
        command='/TEST/LIB/ALBA/ALBA_cnvimg/proc/ALBA_cnvimg.sh ',
        dag=dag,
        doc_md="""
        **ALBA Convert Images Task**
        
        - Converts images after successful rename
        - Part of main image processing pipeline
        """
    )
    
    # Step 3: Convert loaded images (depends on convert success)
    alba_cnvldimg = SSHOperator(
        task_id='alba_cnvldimg',
        ssh_conn_id=SSH_CONN_ID_1,
        command='/TEST/LIB/ALBA/ALBA_cnvldimg/proc/ALBA_cnvldimg.sh ',
        dag=dag,
        doc_md="""
        **ALBA Convert Loaded Images Task**
        
        - Final image conversion step
        - Processes loaded images
        """
    )
    
    # Error handling: Mail errors if rename fails
    alba_mail_errors_renimg = SSHOperator(
        task_id='alba_mail_errors_renimg',
        ssh_conn_id=SSH_CONN_ID_1,
        command='/TEST/LIB/ALBA/ALBA_oper/proc/ALBA_mail_errors_renimg.sh ',
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
        task_id='alba_success_renimg',
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
        task_id='alba_mail_images',
        ssh_conn_id=SSH_CONN_ID_2,
        command='/TEST/LIB/ALBA/ALBA_oper/proc/ALBA_mail_images.sh ',
        dag=dag,
        doc_md="""
        **ALBA Mail Images Summary Task**
        
        - Sends summary email of image processing results
        - Final task in image processing workflow
        """
    )

# XML Processing Task (depends on both XML and no-images file watchers)
alba_mv_xml = SSHOperator(
    task_id='alba_mv_xml',
    ssh_conn_id=SSH_CONN_ID_2,
    command='/TEST/LIB/ALBA/ALBA_oper/proc/ALBA_mv_XML2IArcanum.sh ',
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