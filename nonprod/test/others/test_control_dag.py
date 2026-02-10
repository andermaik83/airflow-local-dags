"""
Test DAG for ON HOLD / ON ICE / Email Alert Policy Testing (Airflow 3.1)

This DAG contains 3 dummy tasks to test the task control policy:
- Use Variables to add tasks to task_hold_list or task_ice_list
- Observe behavior in Airflow UI and logs

Test patterns:
  - Hold task 1: td_test_control.task_1
  - Hold task 2: td_test_control.task_2  
  - Hold all:    td_test_control.*
  - Ice task 3:  td_test_control.task_3
"""
from datetime import datetime, timedelta
from airflow import DAG
# Airflow 3.1 imports
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import logging
import os
import sys

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from utils.common_utils import get_environment_from_path

# Get environment from current DAG path
ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]  # 't' for test

logger = logging.getLogger(__name__)


def print_task_info(task_number, **context):
    """Print task execution info"""
    try:
        print("=" * 60)
        print(f"Task {task_number} is executing!")
        print(f"Task Number: {task_number}")
        
        if context:
            ti = context.get('ti')
            if ti:
                print(f"DAG: {ti.dag_id}")
                print(f"Task: {ti.task_id}")
            
            logical_date = context.get('logical_date')
            if logical_date:
                print(f"Logical Date: {logical_date}")
        
        print("=" * 60)
        print(f"\n✅ Task {task_number} completed successfully!\n")
        return f"Task {task_number} executed successfully"
    except Exception as e:
        print(f"ERROR in task {task_number}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def failing_task(**context):
    """Task that intentionally fails to test email alerts"""
    raise Exception("Intentional failure to test email alerting!")


# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id=f'{env_pre}d_test_control',
    default_args=default_args,
    description='Test DAG for ON HOLD, ON ICE, and Email Alert policy validation',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=[env, 'test', 'control-policy'],
    doc_md="""
    # Task Control Policy Test DAG (Airflow 3.1)
    
    This DAG contains tasks to test ON HOLD, ON ICE, and Email Alert functionality.
    
    ## How to Test:
    
    ### Test ON HOLD (Task waits indefinitely):
    ```bash
    # Hold task 1
    kubectl exec -n airflow deployment/airflow-scheduler -- \\
      airflow variables set task_hold_list '["td_test_control.task_1"]' --json
    
    # Trigger DAG - task_1 will be held
    # Remove from hold to continue
    kubectl exec -n airflow deployment/airflow-scheduler -- \\
      airflow variables set task_hold_list '[]' --json
    ```
    
    ### Test ON ICE (Task skipped):
    ```bash
    # Ice task 3
    kubectl exec -n airflow deployment/airflow-scheduler -- \\
      airflow variables set task_ice_list '["td_test_control.task_3"]' --json
    
    # Trigger DAG - task_3 will be skipped
    ```
    
    ### Test Email Alert (task_fail):
    ```bash
    # Set email recipients
    kubectl exec -n airflow deployment/airflow-scheduler -- \\
      airflow variables set alert_email_recipients "your@email.com"
    
    # Set environment to prod
    kubectl set env deployment/airflow-worker -n airflow AIRFLOW_ENVIRONMENT=prod
    
    # Trigger DAG - task_fail will send email
    ```
    
    ### Test Wildcards:
    ```bash
    # Hold all tasks in this DAG
    kubectl exec -n airflow deployment/airflow-scheduler -- \\
      airflow variables set task_hold_list '["td_test_control.*"]' --json
    ```
    """
) as dag:

    # Task 1 - Start task
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=print_task_info,
        op_kwargs={'task_number': 1},
        doc_md="**Task 1 - Start Task**\n\nTest Pattern: `td_test_control.task_1`"
    )

    # Task 2 - Middle task
    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=print_task_info,
        op_kwargs={'task_number': 2},
        doc_md="**Task 2 - Middle Task**\n\nTest Pattern: `td_test_control.task_2`"
    )

    # Task 3 - End task
    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=print_task_info,
        op_kwargs={'task_number': 3},
        doc_md="**Task 3 - End Task**\n\nTest Pattern: `td_test_control.task_3`"
    )

    # Task Fail - For testing email alerts
    task_fail = PythonOperator(
        task_id='task_fail',
        python_callable=failing_task,
        doc_md="**Task Fail - Email Alert Test**\n\nThis task intentionally fails to test email alerting."
    )

    # Dependencies
    task_1 >> task_2 >> task_3
    task_3 >> task_fail  # task_fail runs after task_3