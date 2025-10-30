"""
SSH Utility Functions for Airflow DAGs
Shared functions for SSH operations and file checking across multiple DAGs
"""

import logging
import os
from typing import Optional, Dict, Any


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


# SSH Connection ID constants for reuse across DAGs
class SSHConnections:
    """Constants for SSH connection IDs used across ALBA and SLRE DAGs"""
    TGEN_VL101 = 'tgen_vl101'  # Main Linux processing server
    TGEN_VL105 = 'tgen_vl105'  # File monitoring server
    TOPR_VW103 = 'topr_vw103'  # Windows batch processing server
    
def get_file_check_command():
    """Add the missing function implementation"""
    # Your implementation here
    pass