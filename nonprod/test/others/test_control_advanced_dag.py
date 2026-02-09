"""
Advanced Test DAG for ON HOLD / ON ICE Policy Testing

This DAG contains complex workflows to thoroughly test task control:
- Task Groups (nested)
- Parallel branches
- Diamond patterns
- Fan-out / Fan-in patterns

Test Patterns Examples:
  Single task:     td_test_control_adv.start
  Task in group:   td_test_control_adv.extract.extract_api
  Entire group:    td_test_control_adv.extract.*
  All transform:   td_test_control_adv.transform.*
  Multiple tasks:  ["td_test_control_adv.task_a", "td_test_control_adv.task_b"]
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import logging
import os
import sys
import time

# Add path for importing shared utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from utils.common_utils import get_environment_from_path

# Get environment from current DAG path
ENV = get_environment_from_path(__file__)
env = ENV.lower()
env_pre = env[0]  # 't' for test

logger = logging.getLogger(__name__)


def simulate_task(task_name, duration=2, **context):
    """Simulate task execution with logging"""
    ti = context.get('ti')
    full_id = f"{ti.dag_id}.{ti.task_id}" if ti else task_name
    
    print("=" * 70)
    print(f"🚀 EXECUTING: {full_id}")
    print(f"   Task Name: {task_name}")
    print(f"   Duration: {duration}s")
    print("=" * 70)
    
    time.sleep(duration)
    
    print(f"✅ COMPLETED: {full_id}")
    return f"{task_name} done"


def failing_task(task_name, **context):
    """Task that fails - for testing error scenarios"""
    raise Exception(f"Task {task_name} failed intentionally!")


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
    dag_id=f'{env_pre}d_test_control_adv',
    default_args=default_args,
    description='Advanced Test DAG for ON HOLD/ON ICE with TaskGroups and complex flows',
    schedule=None,
    catchup=False,
    tags=[env, 'test', 'control-policy', 'advanced'],
    doc_md="""
# Advanced Task Control Policy Test DAG

## Structure Overview:
```
                    ┌─────────────────────────────────────────────────────────┐
                    │                      EXTRACT GROUP                       │
                    │  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
                    │  │extract_db│  │extract_api│ │extract_file│              │
                    │  └────┬─────┘  └─────┬─────┘ └─────┬─────┘              │
                    └───────┼──────────────┼─────────────┼────────────────────┘
                            │              │             │
          ┌─────────────────┴──────────────┴─────────────┴─────────────────┐
          │                        TRANSFORM GROUP                          │
          │   ┌──────────────────────────────────────────────────────┐     │
          │   │                    clean subgroup                     │     │
          │   │  ┌─────────┐  ┌─────────┐  ┌─────────┐               │     │
          │   │  │clean_db │  │clean_api│  │clean_file│              │     │
          │   │  └────┬────┘  └────┬────┘  └────┬────┘               │     │
          │   └───────┴────────────┴────────────┴────────────────────┘     │
          │                        │                                        │
          │   ┌──────────────────────────────────────────────────────┐     │
          │   │                   enrich subgroup                     │     │
          │   │  ┌──────────┐  ┌──────────┐                          │     │
          │   │  │enrich_geo│  │enrich_time│                         │     │
          │   │  └────┬─────┘  └─────┬────┘                          │     │
          │   └───────┴──────────────┴───────────────────────────────┘     │
          └───────────────────────────┬────────────────────────────────────┘
                                      │
    ┌─────────────────────────────────┼─────────────────────────────────┐
    │                                 │                                 │
    ▼                                 ▼                                 ▼
┌────────┐                      ┌──────────┐                      ┌────────┐
│load_dwh│                      │load_lake │                      │load_api│
└───┬────┘                      └────┬─────┘                      └───┬────┘
    │                                │                                 │
    └────────────────────────────────┼─────────────────────────────────┘
                                     ▼
                               ┌──────────┐
                               │  notify  │
                               └────┬─────┘
                                    ▼
                               ┌──────────┐
                               │   end    │
                               └──────────┘
```

## Test Scenarios:

### 1. Hold Single Task
```bash
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_hold_list '["td_test_control_adv.start"]' --json
```

### 2. Hold Task Inside Group
```bash
# Hold one extract task
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_hold_list '["td_test_control_adv.extract.extract_api"]' --json
```

### 3. Hold Entire Group (Wildcard)
```bash
# Hold all extract tasks
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_hold_list '["td_test_control_adv.extract.*"]' --json
```

### 4. Hold Nested Subgroup
```bash
# Hold all clean tasks in transform group
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_hold_list '["td_test_control_adv.transform.clean.*"]' --json
```

### 5. Skip Task (ON ICE)
```bash
# Skip one load task - others continue
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_ice_list '["td_test_control_adv.load_dwh"]' --json
```

### 6. Skip Task in Group (ON ICE)
```bash
# Skip extract_file - extract_db and extract_api continue
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_ice_list '["td_test_control_adv.extract.extract_file"]' --json
```

### 7. Mixed Hold and Ice
```bash
# Hold transform, skip one load
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_hold_list '["td_test_control_adv.transform.*"]' --json
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_ice_list '["td_test_control_adv.load_api"]' --json
```

### 8. Release from Hold
```bash
# Clear hold list to release all
kubectl exec -n airflow-local deployment/airflow-scheduler -- \\
  airflow variables set task_hold_list '[]' --json
```

## Expected Behaviors:

| Scenario | Status | Downstream |
|----------|--------|------------|
| ON HOLD | `deferred` | Waits |
| ON ICE | `skipped` | Continues |
| Released | `running` → `success` | Proceeds |

## Monitoring:
```bash
# Watch task states
kubectl logs -n airflow-local -l component=worker -f | grep -E "HOLD|ICE|EXECUTING"

# Check trigger status
kubectl logs -n airflow-local -l component=triggerer -f
```
    """,
) as dag:

    # =========================================================================
    # START
    # =========================================================================
    start = PythonOperator(
        task_id='start',
        python_callable=simulate_task,
        op_kwargs={'task_name': 'start', 'duration': 1},
        doc_md="**Start task** - Entry point. Pattern: `td_test_control_adv.start`"
    )

    # =========================================================================
    # EXTRACT TASK GROUP - Parallel extraction from multiple sources
    # =========================================================================
    with TaskGroup(group_id='extract', tooltip='Extract data from multiple sources') as extract_group:
        
        extract_db = PythonOperator(
            task_id='extract_db',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'extract_db', 'duration': 3},
            doc_md="Extract from database. Pattern: `td_test_control_adv.extract.extract_db`"
        )
        
        extract_api = PythonOperator(
            task_id='extract_api',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'extract_api', 'duration': 2},
            doc_md="Extract from API. Pattern: `td_test_control_adv.extract.extract_api`"
        )
        
        extract_file = PythonOperator(
            task_id='extract_file',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'extract_file', 'duration': 1},
            doc_md="Extract from files. Pattern: `td_test_control_adv.extract.extract_file`"
        )

    # =========================================================================
    # TRANSFORM TASK GROUP - With nested subgroups
    # =========================================================================
    with TaskGroup(group_id='transform', tooltip='Transform and enrich data') as transform_group:
        
        # Nested group: Clean
        with TaskGroup(group_id='clean', tooltip='Data cleaning tasks') as clean_subgroup:
            
            clean_db = PythonOperator(
                task_id='clean_db',
                python_callable=simulate_task,
                op_kwargs={'task_name': 'clean_db', 'duration': 2},
                doc_md="Clean DB data. Pattern: `td_test_control_adv.transform.clean.clean_db`"
            )
            
            clean_api = PythonOperator(
                task_id='clean_api',
                python_callable=simulate_task,
                op_kwargs={'task_name': 'clean_api', 'duration': 2},
                doc_md="Clean API data. Pattern: `td_test_control_adv.transform.clean.clean_api`"
            )
            
            clean_file = PythonOperator(
                task_id='clean_file',
                python_callable=simulate_task,
                op_kwargs={'task_name': 'clean_file', 'duration': 1},
                doc_md="Clean file data. Pattern: `td_test_control_adv.transform.clean.clean_file`"
            )
        
        # Nested group: Enrich
        with TaskGroup(group_id='enrich', tooltip='Data enrichment tasks') as enrich_subgroup:
            
            enrich_geo = PythonOperator(
                task_id='enrich_geo',
                python_callable=simulate_task,
                op_kwargs={'task_name': 'enrich_geo', 'duration': 2},
                doc_md="Add geo data. Pattern: `td_test_control_adv.transform.enrich.enrich_geo`"
            )
            
            enrich_time = PythonOperator(
                task_id='enrich_time',
                python_callable=simulate_task,
                op_kwargs={'task_name': 'enrich_time', 'duration': 1},
                doc_md="Add time data. Pattern: `td_test_control_adv.transform.enrich.enrich_time`"
            )
        
        # Internal dependencies within transform group
        clean_subgroup >> enrich_subgroup

    # =========================================================================
    # LOAD TASKS - Parallel loading (fan-out pattern)
    # =========================================================================
    load_dwh = PythonOperator(
        task_id='load_dwh',
        python_callable=simulate_task,
        op_kwargs={'task_name': 'load_dwh', 'duration': 3},
        doc_md="Load to Data Warehouse. Pattern: `td_test_control_adv.load_dwh`"
    )
    
    load_lake = PythonOperator(
        task_id='load_lake',
        python_callable=simulate_task,
        op_kwargs={'task_name': 'load_lake', 'duration': 2},
        doc_md="Load to Data Lake. Pattern: `td_test_control_adv.load_lake`"
    )
    
    load_api = PythonOperator(
        task_id='load_api',
        python_callable=simulate_task,
        op_kwargs={'task_name': 'load_api', 'duration': 1},
        doc_md="Load to API. Pattern: `td_test_control_adv.load_api`"
    )

    # =========================================================================
    # NOTIFY & END - Fan-in pattern
    # =========================================================================
    notify = PythonOperator(
        task_id='notify',
        python_callable=simulate_task,
        op_kwargs={'task_name': 'notify', 'duration': 1},
        trigger_rule='none_failed_min_one_success',  # Continue if at least one load succeeded
        doc_md="Send notifications. Pattern: `td_test_control_adv.notify`"
    )
    
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
        doc_md="End marker. Pattern: `td_test_control_adv.end`"
    )

    # =========================================================================
    # PARALLEL BRANCH - Independent processing path
    # =========================================================================
    with TaskGroup(group_id='parallel_branch', tooltip='Independent parallel processing') as parallel_branch:
        
        branch_a = PythonOperator(
            task_id='branch_a',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'branch_a', 'duration': 2},
            doc_md="Branch A. Pattern: `td_test_control_adv.parallel_branch.branch_a`"
        )
        
        branch_b = PythonOperator(
            task_id='branch_b',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'branch_b', 'duration': 3},
            doc_md="Branch B. Pattern: `td_test_control_adv.parallel_branch.branch_b`"
        )
        
        branch_merge = PythonOperator(
            task_id='branch_merge',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'branch_merge', 'duration': 1},
            doc_md="Merge branches. Pattern: `td_test_control_adv.parallel_branch.branch_merge`"
        )
        
        [branch_a, branch_b] >> branch_merge

    # =========================================================================
    # DIAMOND PATTERN - For testing complex dependencies
    # =========================================================================
    with TaskGroup(group_id='diamond', tooltip='Diamond dependency pattern') as diamond_group:
        
        diamond_top = PythonOperator(
            task_id='top',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'diamond_top', 'duration': 1},
            doc_md="Diamond top. Pattern: `td_test_control_adv.diamond.top`"
        )
        
        diamond_left = PythonOperator(
            task_id='left',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'diamond_left', 'duration': 2},
            doc_md="Diamond left. Pattern: `td_test_control_adv.diamond.left`"
        )
        
        diamond_right = PythonOperator(
            task_id='right',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'diamond_right', 'duration': 2},
            doc_md="Diamond right. Pattern: `td_test_control_adv.diamond.right`"
        )
        
        diamond_bottom = PythonOperator(
            task_id='bottom',
            python_callable=simulate_task,
            op_kwargs={'task_name': 'diamond_bottom', 'duration': 1},
            doc_md="Diamond bottom. Pattern: `td_test_control_adv.diamond.bottom`"
        )
        
        diamond_top >> [diamond_left, diamond_right] >> diamond_bottom

    # =========================================================================
    # MAIN WORKFLOW DEPENDENCIES
    # =========================================================================
    
    # Main ETL pipeline
    start >> extract_group >> transform_group >> [load_dwh, load_lake, load_api] >> notify >> end
    
    # Parallel branch (independent path from start)
    start >> parallel_branch >> notify
    
    # Diamond pattern (independent path from start)
    start >> diamond_group >> notify
