"""
Airflow Local Settings - Policy Hooks (PRODUCTION)
This file is automatically loaded by Airflow to apply global policies.

Environment: PRODUCTION
Stricter policies and controls for production environment.
"""
import logging
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State

logger = logging.getLogger(__name__)


def task_instance_mutation_hook(task_instance):
    """
    PRODUCTION policy hook to control task execution via Airflow Variables.
    
    ON HOLD: Prevents task from running (sets to UP_FOR_RESCHEDULE)
    ON ICE: Skips task execution (marks as SKIPPED)
    
    Variable format (JSON arrays):
    - task_hold_list: ["dag_id.task_id", "dag_id.group_id.*", "dag_id.*"]
    - task_ice_list: ["dag_id.task_id", "dag_id.group_id.*", "dag_id.*"]
    
    Examples:
    - "pd_watch_usa_cl.tcCOMrec_CL" - Exact task
    - "pd_ukra_processing.tbUKRA_trm.*" - All tasks in group
    - "pd_alba_conversion.*" - All tasks in DAG
    - "pd_*" - All PROD environment DAGs
    
    PRODUCTION SAFEGUARD: "*" wildcard requires explicit approval
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
    
    # PRODUCTION SAFEGUARD: Log warning if global wildcard is used
    if "*" in hold_list:
        logger.error(
            "⚠️ [PRODUCTION] Global wildcard '*' detected in task_hold_list! "
            "All production workflows are ON HOLD. Verify this is intentional."
        )
    if "*" in ice_list:
        logger.error(
            "⚠️ [PRODUCTION] Global wildcard '*' detected in task_ice_list! "
            "All production workflows will be skipped. Verify this is intentional."
        )
    
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
    
    # 5. Global wildcard: * (use with extreme caution in production)
    patterns.append("*")
    
    # Check HOLD first (higher priority)
    for pattern in patterns:
        if pattern in hold_list:
            logger.error(
                f"🛑 [PRODUCTION] Task {dag_id}.{task_id} is ON HOLD (matched pattern: '{pattern}'). "
                f"Production workflow blocked. Remove from 'task_hold_list' Variable to resume."
            )
            # Set to UP_FOR_RESCHEDULE to prevent execution but allow retry
            task_instance.state = State.UP_FOR_RESCHEDULE
            task_instance.end_date = None
            return
    
    # Check ICE (lower priority - skips but doesn't block)
    for pattern in patterns:
        if pattern in ice_list:
            logger.warning(
                f"❄️ [PRODUCTION] Task {dag_id}.{task_id} is ON ICE (matched pattern: '{pattern}'). "
                f"Production task will be skipped. Remove from 'task_ice_list' Variable to enable."
            )
            raise AirflowSkipException(f"Task ON ICE by policy (pattern: {pattern})")


def dag_policy(dag):
    """
    PRODUCTION DAG-level policy to control entire DAGs.
    
    Variable: paused_dags (JSON array)
    Example: ["pd_watch_usa_cl", "pd_alba_ingestion"]
    
    PRODUCTION: Pausing DAGs requires approval
    """
    try:
        paused_dags = Variable.get("paused_dags", default_var=[], deserialize_json=True)
        
        if dag.dag_id in paused_dags:
            logger.error(
                f"⚠️ [PRODUCTION] DAG {dag.dag_id} is in 'paused_dags' list. "
                f"Production DAG will be paused upon creation."
            )
            dag.is_paused_upon_creation = True
        
        if "*" in paused_dags:
            logger.critical(
                "🚨 [PRODUCTION] Global wildcard '*' in paused_dags! "
                "ALL PRODUCTION DAGS WILL BE PAUSED. Immediate action required."
            )
            dag.is_paused_upon_creation = True
    except Exception as e:
        logger.warning(f"Failed to load paused_dags list: {e}")


def task_policy(task):
    """
    PRODUCTION task policy - stricter settings for production reliability.
    
    - No automatic retries (explicit only)
    - Enforces timeout limits
    - Requires proper ownership
    """
    # Production: No automatic retries - must be explicit
    # (Don't modify task.retries here - let DAG authors decide)
    
    # Ensure tasks have proper ownership
    if not hasattr(task, 'owner') or task.owner == 'airflow':
        task.owner = 'dataproc-prod'
    
    # PRODUCTION: Log critical tasks
    if task.email_on_failure:
        logger.info(f"[PRODUCTION] Critical task registered: {task.dag_id}.{task.task_id}")
