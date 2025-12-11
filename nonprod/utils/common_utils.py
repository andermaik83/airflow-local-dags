"""
SSH Utility Functions for Airflow DAGs
Shared functions for SSH operations and file checking across multiple DAGs
"""

import logging
import os
from typing import Optional, Dict, Any, Callable


def get_environment_from_path(file_path: str) -> str:
    """
    Extract environment from DAG file path
    
    Args:
        file_path (str): Path to the DAG file (__file__)
        
    Returns:
        str: Environment (TEST, PROD, ACPT, etc.)
        
    Example:
        # From /path/to/nonprod/test/alba/alba_dag.py -> TEST
        # From /path/to/nonprod/acpt/alba/alba_dag.py -> ACPT
        # From /path/to/prod/alba/alba_dag.py -> PROD
    """
    try:
        path_parts = file_path.replace('\\', '/').split('/')
        
        # Check for specific environment in path
        if 'acpt' in path_parts:
            return 'ACPT'
        elif 'test' in path_parts:
            return 'TEST'
        elif 'prod' in path_parts:
            return 'PROD'
        elif 'nonprod' in path_parts:
            # If nonprod but no specific env, default to TEST
            return 'TEST'
        else:
            return 'TEST'  # Default to TEST
    except Exception:
        return 'TEST'  # Default fallback


def check_file_exists(filepath: str) -> str:
    """
    Generate SSH command to check if file exists - for use with SSHOperator
    
    Args:
        filepath (str): Remote file path to check
        
    Returns:
        str: SSH command that exits with code 0 if file exists, 1 if not
        
    Example:
        # Use with SSHOperator
        check_task = SSHOperator(
            task_id='check_file_exists',
            ssh_conn_id='my_ssh_conn',
            command=check_file_exists('/path/to/file')
        )
    """
    return f'test -f {filepath}'


def check_file_pattern(file_pattern: str) -> str:
    """
    Generate SSH command to check if files matching pattern exist - for use with SSHOperator
    
    Args:
        file_pattern (str): File pattern to check (supports glob patterns)
        
    Returns:
        str: SSH command that exits with code 0 if files exist, 1 if not
        
    Example:
        # Use with SSHOperator
        check_task = SSHOperator(
            task_id='check_pattern',
            ssh_conn_id='my_ssh_conn',
            command=check_file_pattern('/path/to/batchproc*')
        )
    """
    return f'ls {file_pattern} >/dev/null 2>&1'


def check_directory(dirpath: str) -> str:
    """
    Generate SSH command to check if directory exists - for use with SSHOperator
    
    Args:
        dirpath (str): Remote directory path to check
        
    Returns:
        str: SSH command that exits with code 0 if directory exists, 1 if not
        
    Example:
        # Use with SSHOperator
        check_task = SSHOperator(
            task_id='check_dir',
            ssh_conn_id='my_ssh_conn',
            command=check_directory('/path/to/directory')
        )
    """
    return f'test -d {dirpath}'


def get_file_size(filepath: str) -> str:
    """
    Generate SSH command to get file size - for use with SSHOperator
    
    Args:
        filepath (str): Remote file path
        
    Returns:
        str: SSH command that outputs file size in bytes
        
    Example:
        # Use with SSHOperator
        size_task = SSHOperator(
            task_id='get_size',
            ssh_conn_id='my_ssh_conn',
            command=get_file_size('/path/to/file')
        )
    """
    return f'stat -c%s {filepath} 2>/dev/null || echo "0"'


def wait_for_stable_file(filepath: str, stable_seconds: int = 60) -> str:
    """
    Generate SSH command to wait for file to be stable - for use with SSHOperator
    
    Args:
        filepath (str): Remote file path to monitor
        stable_seconds (int): Seconds to wait for stable file size
        
    Returns:
        str: SSH command that waits for file stability
        
    Example:
        # Use with SSHOperator
        wait_task = SSHOperator(
            task_id='wait_stable',
            ssh_conn_id='my_ssh_conn',
            command=wait_for_stable_file('/path/to/file', 60)
        )
    """
    return f'''
size1=$(stat -c%s {filepath} 2>/dev/null || echo "0")
sleep {stable_seconds}
size2=$(stat -c%s {filepath} 2>/dev/null || echo "0")
if [ "$size1" = "$size2" ] && [ "$size1" != "0" ]; then
    echo "File is stable: $size1 bytes"
    exit 0
else
    echo "File is not stable or missing: $size1 -> $size2"
    exit 1
fi
'''.strip()

# ================= Environment-aware Connection Resolver =================
# Unified logical connection names -> environment-specific Airflow connection IDs
_CONN_MAP: Dict[str, Dict[str, str]] = {
    'nvs_cnt1': {'PROD': 'popr_vl101', 'TEST': 'tgen_vl105'},
    'nvs_cnt2': {'PROD': 'popr_vl102', 'TEST': 'tgen_vl105'},
    'sea_run1': {'PROD': 'popr_vl113', 'TEST': 'tgen_vl105', 'ACPT': 'agen_vl111'},
    'sea_run2': {'PROD': 'popr_vl113', 'TEST': 'tgen_vl105', 'ACPT': 'agen_vl111'},
    'sea_upd':  {'PROD': 'popr_vl112', 'TEST': 'tgen_vl106'},
    'sea_upd2': {'PROD': 'popr_vl112', 'TEST': 'tgen_vl106'},
    'small':    {'PROD': 'popr_vl113', 'TEST': 'tgen_vl105'},
    'opr_vl101':{'PROD': 'popr_vl101', 'TEST': 'tgen_vl101', 'ACPT': 'agen_vl101'},
    'opr_vl102':{'PROD': 'popr_vl102', 'TEST': 'tgen_vl101', 'ACPT': 'agen_vl101'},
    'opr_vl103':{'PROD': 'popr_vl103', 'TEST': 'tgen_vl101', 'ACPT': 'agen_vl101'},
    'opr_vl107':{'PROD': 'popr_vl107', 'TEST': 'tgen_vl101', 'ACPT': 'agen_vl101'},
    'opr_vl111':{'PROD': 'popr_vl111', 'TEST': 'tgen_vl105', 'ACPT': 'agen_vl111'},
    'opr_vl112':{'PROD': 'popr_vl112', 'TEST': 'tgen_vl106', 'ACPT': 'agen_vl111'},
    'opr_vl113':{'PROD': 'popr_vl113', 'TEST': 'tgen_vl105', 'ACPT': 'agen_vl111'},
    'opr_vw104':{'PROD': 'popr_vw104', 'TEST': 'topr_vw103', 'ACPT': 'aopr_vw102'},
    'opr_vw105':{'PROD': 'popr_vw105', 'TEST': 'topr_vw103', 'ACPT': 'aopr_vw102'},
}

def resolve_connection_id(env_name: str, conn_name: str) -> str:
    """Resolve logical conn_name into environment-specific Airflow connection ID.

    If no exact environment mapping is found, falls back to TEST mapping, then returns conn_name itself.
    """
    env_key = env_name.upper() if env_name else 'TEST'
    env_map = _CONN_MAP.get(conn_name)
    if not env_map:
        return conn_name  # pass-through
    return env_map.get(env_key) or env_map.get('TEST') or conn_name