"""
Test DAG for ON HOLD / ON ICE Policy Testing

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
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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
            
            exec_date = context.get('execution_date')
            if exec_date:
                print(f"Execution Date: {exec_date}")
        
        print("=" * 60)
        print(f"\n✅ Task {task_number} completed successfully!\n")
        return f"Task {task_number} executed successfully"
    except Exception as e:
        print(f"ERROR in task {task_number}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    dag_id=f'{env_pre}d_test_control',
    default_args=default_args,
    description='Test DAG for ON HOLD and ON ICE policy validation',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=[env, 'test', 'control-policy'],
    doc_md="""
    # Task Control Policy Test DAG
    
    This DAG contains 3 dummy tasks to test ON HOLD and ON ICE functionality.
    
    ## How to Test:
    
    ### Test ON HOLD (Task waits indefinitely):
    ```bash
    # Hold task 1
    kubectl exec -n airflow-local deployment/airflow-scheduler -- \
      airflow variables set task_hold_list '["td_test_control.task_1"]' --json
    
    # Trigger DAG - task_1 will be held
    # Remove from hold to continue
    kubectl exec -n airflow-local deployment/airflow-scheduler -- \
      airflow variables set task_hold_list '[]' --json
    ```
    
    ### Test ON ICE (Task skipped):
    ```bash
    # Ice task 3
    kubectl exec -n airflow-local deployment/airflow-scheduler -- \
      airflow variables set task_ice_list '["td_test_control.task_3"]' --json
    
    # Trigger DAG - task_3 will be skipped
    ```
    
    ### Test Wildcards:
    ```bash
    # Hold all tasks in this DAG
    kubectl exec -n airflow-local deployment/airflow-scheduler -- \
      airflow variables set task_hold_list '["td_test_control.*"]' --json
    ```
    
    ## Expected Behavior:
    - **ON HOLD**: Task shows as "up_for_reschedule" state, doesn't execute
    - **ON ICE**: Task shows as "skipped" state, downstream tasks still run
    
    ## Check Logs:
    ```bash
    kubectl logs -n airflow-local -l component=scheduler --tail=100 | grep "ON HOLD\|ON ICE"
    ```
    """
)

# Task 1 - Start task
task_1 = PythonOperator(
    task_id='task_1',
    python_callable=print_task_info,
    op_kwargs={'task_number': 1},
    dag=dag,
    doc_md="""
    **Task 1 - Start Task**
    
    First task in the sequence. Use this to test ON HOLD behavior.
    
    **Test Pattern:** `td_test_control.task_1`
    """
)

# Task 2 - Middle task
task_2 = PythonOperator(
    task_id='task_2',
    python_callable=print_task_info,
    op_kwargs={'task_number': 2},
    dag=dag,
    doc_md="""
    **Task 2 - Middle Task**
    
    Middle task in the sequence. Depends on task_1.
    
    **Test Pattern:** `td_test_control.task_2`
    """
)

# Task 3 - End task
task_3 = PythonOperator(
    task_id='task_3',
    python_callable=print_task_info,
    op_kwargs={'task_number': 3},
    dag=dag,
    doc_md="""
    **Task 3 - End Task**
    
    Final task in the sequence. Use this to test ON ICE behavior.
    
    **Test Pattern:** `td_test_control.task_3`
    
    When ON ICE, this task will be skipped but won't block the workflow.
    """
)

# Simple sequential dependencies
task_1 >> task_2 >> task_3
