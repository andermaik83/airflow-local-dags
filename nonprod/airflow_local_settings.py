"""
Airflow Local Settings - Policy Hooks (NON-PRODUCTION)
This file is automatically loaded by Airflow to apply global policies.

Environment: TEST, ACPT
"""
import logging
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State

logger = logging.getLogger(__name__)


def task_instance_mutation_hook(task_instance):
    """
    NON-PROD policy hook to control task execution via Airflow Variables.
    
    ON HOLD: Prevents task from running (sets to UP_FOR_RESCHEDULE)
    ON ICE: Skips task execution (marks as SKIPPED)
    
    Variable format (JSON arrays):
    - task_hold_list: ["dag_id.task_id", "dag_id.group_id.*", "dag_id.*"]
    - task_ice_list: ["dag_id.task_id", "dag_id.group_id.*", "dag_id.*"]
    
    Examples:
    - "td_watch_usa_cl.tcCOMrec_CL" - Exact task
    - "td_ukra_processing.tbUKRA_trm.*" - All tasks in group
    - "td_alba_conversion.*" - All tasks in DAG
    - "*" - All tasks globally (use with caution!)
    """
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    
    # Get task group ID if exists
    tg_id = None
    if task_instance.task and hasattr(task_instance.task, 'task_group'):
        if task_instance.task.task_group:
            tg_id = task_instance.task.task_group.group_id
    
    # Load control lists from Airflow Variables
    try:
        hold_list = Variable.get("task_hold_list", default_var=[], deserialize_json=True)
        ice_list = Variable.get("task_ice_list", default_var=[], deserialize_json=True)
    except Exception as e:
        logger.warning(f"Failed to load task control lists: {e}")
        return
    
    # Build match patterns (most specific to least specific)
    patterns = []
    
    # 1. Exact match with task group: dag_id.group_id.task_id
    if tg_id:
        patterns.append(f"{dag_id}.{tg_id}.{task_id}")
    
    # 2. Exact match without group: dag_id.task_id
    patterns.append(f"{dag_id}.{task_id}")
    
    # 3. Task group wildcard: dag_id.group_id.*
    if tg_id:
        patterns.append(f"{dag_id}.{tg_id}.*")
    
    # 4. DAG wildcard: dag_id.*
    patterns.append(f"{dag_id}.*")
    
    # 5. Global wildcard: *
    patterns.append("*")
    
    # Check HOLD first (higher priority)
    for pattern in patterns:
        if pattern in hold_list:
            logger.warning(
                f"🛑 Task {dag_id}.{task_id} is ON HOLD (matched pattern: '{pattern}'). "
                f"Remove from 'task_hold_list' Variable to resume."
            )
            # Set to UP_FOR_RESCHEDULE to prevent execution but allow retry
            task_instance.state = State.UP_FOR_RESCHEDULE
            task_instance.end_date = None
            return
    
    # Check ICE (lower priority - skips but doesn't block)
    for pattern in patterns:
        if pattern in ice_list:
            logger.warning(
                f"❄️ Task {dag_id}.{task_id} is ON ICE (matched pattern: '{pattern}'). "
                f"Remove from 'task_ice_list' Variable to enable."
            )
            raise AirflowSkipException(f"Task ON ICE by policy (pattern: {pattern})")


def dag_policy(dag):
    """
    Optional: DAG-level policy to control entire DAGs.
    
    Variable: paused_dags (JSON array)
    Example: ["td_watch_usa_cl", "ad_alba_ingestion"]
    """
    try:
        paused_dags = Variable.get("paused_dags", default_var=[], deserialize_json=True)
        
        if dag.dag_id in paused_dags or "*" in paused_dags:
            logger.warning(f"DAG {dag.dag_id} is in 'paused_dags' list - marking as paused")
            dag.is_paused_upon_creation = True
    except Exception as e:
        logger.warning(f"Failed to load paused_dags list: {e}")


# Optional: Task policy (runs before task creation)
def task_policy(task):
    """
    Optional: Modify task properties before they're created.
    Example: Set default retries, timeouts, etc.
    """
    # Example: Set default retries for all SSHOperator tasks
    if task.task_type == "SSHOperator":
        if task.retries == 0:
            task.retries = 1
    
    # Example: Add owner tag to all tasks
    if not hasattr(task, 'owner') or task.owner == 'airflow':
        task.owner = 'dataproc'
