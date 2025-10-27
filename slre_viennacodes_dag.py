from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import logging

# DAG Definition
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # alarm_if_fail: 1 for viennacodes tasks
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'slre_viennacodes_dag',
    default_args=default_args,
    description='SLRE Vienna Codes Processing Pipeline',
    schedule=None,  # Triggered by file sensor tfSLRE_start_VCD
    catchup=False,
    tags=['test', 'slre', 'dataproc', 'viennacodes'],
)

# SSH Connection IDs
SSH_CONN_ID_1 = 'tgen_vl101'  # Linux processing server
SSH_CONN_ID_3 = 'topr_vw103'  # Windows server for batch processing

# TaskGroup representing BOX tbSLRE_viennacodes
with TaskGroup(group_id='tbSLRE_viennacodes', dag=dag) as viennacodes_taskgroup:
    
    # tcSLRE_cnvviennacodexml - First task in the BOX
    slre_cnvviennacodexml = SSHOperator(
        task_id='tcSLRE_cnvviennacodexml',
        ssh_conn_id=SSH_CONN_ID_1,
        command='/TEST/LIB/SLRE/SLRE_cnvviennacodexml/proc/SLRE_cnvviennacodexml.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Convert Vienna Code XML Task**
        
        **Purpose:**
        - Converts Vienna code data to XML format
        - First step in Vienna codes processing pipeline
        """
    )
    
    # tcSLRE_mv2bpvc - Move to batch processing, depends on cnvviennacodexml
    slre_mv2bpvc = SSHOperator(
        task_id='tcSLRE_mv2bpvc',
        ssh_conn_id=SSH_CONN_ID_1,
        command='/TEST/LIB/SLRE/SLRE_oper/proc/SLRE_mv2bpvc.sh ',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Move to Batch Processing VC Task**
        
        **Purpose:**
        - Moves Vienna code files to batch processing directory
        - Prepares files for Windows batch processing
        """
    )
    
    # tcSLRE_autobp_VC - Auto batch processing on Windows, depends on mv2bpvc
    slre_autobp_vc = SSHOperator(
        task_id='tcSLRE_autobp_VC',
        ssh_conn_id=SSH_CONN_ID_3,  # Windows server
        command='e:\\Local\\TIPSocr\\proc\\TIPSi_start_AutoBatchproc_VC.cmd SLRE',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Auto Batch Processing VC Task**
        
        **Purpose:**
        - Executes Vienna codes batch processing on Windows server
        - OCR and processing of Vienna code images
        """
    )
    
    # tcSLRE_cleanup_vc - Cleanup Vienna codes, depends on autobp_VC
    slre_cleanup_vc = SSHOperator(
        task_id='tcSLRE_cleanup_vc',
        ssh_conn_id=SSH_CONN_ID_1,
        command='/TEST/LIB/SLRE/SLRE_oper/proc/SLRE_cleanupvc.sh',
        dag=dag,
        email_on_failure=True,  # alarm_if_fail: 1
        doc_md="""
        **SLRE Cleanup Vienna Codes Task**
        
        **Purpose:**
        - Cleanup Vienna codes processing files
        - Final step in Vienna codes workflow
        """
    )
    
    # Define dependencies within the TaskGroup
    slre_cnvviennacodexml >> slre_mv2bpvc >> slre_autobp_vc >> slre_cleanup_vc

# Standalone preparation task for Vienna codes
slre_prepvcd = SSHOperator(
    task_id='tcSLRE_prepvcd',
    ssh_conn_id=SSH_CONN_ID_1,
    command='/TEST/LIB/SLRE/SLRE_oper/proc/SLRE_prepvcd.sh ',
    dag=dag,
    email_on_failure=False,  # alarm_if_fail: 0
    doc_md="""
    **SLRE Prepare VCD Task**
    
    **Purpose:**
    - Prepares Vienna code data for processing
    - Standalone preparation task
    """
)
