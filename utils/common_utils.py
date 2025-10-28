"""
SSH Utility Functions for Airflow DAGs
Shared functions for SSH operations and file checking across multiple DAGs
"""

import logging
import os
from airflow.providers.ssh.hooks.ssh import SSHHook
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


def build_file_path(base_path: str, file_path: str = None) -> str:
    """
    Build environment-specific file path
    
    Args:
        base_path (str): Path to the DAG file (__file__)
        file_path (str): Relative file path after /ENV/SHR/
        
    Returns:
        str: Complete file path with environment
        
    Example:
        build_file_path(__file__, "ALBA/work/ALBA_imgissue.par")
        # Returns: "/TEST/SHR/ALBA/work/ALBA_imgissue.par" (for nonprod)
        # Returns: "/PROD/SHR/ALBA/work/ALBA_imgissue.par" (for prod)
    """
    env = get_environment_from_path(base_path)
    if file_path:
        return f"/{env}/SHR/{file_path}"
    return f"/{env}/SHR"


def check_file_exists(filepath: str, ssh_conn_id: str, **context) -> bool:
    """
    Check if a file exists on remote server via SSH
    
    Args:
        filepath (str): Remote file path to check
        ssh_conn_id (str): SSH connection ID configured in Airflow
        **context: Airflow context (when used as PythonOperator callable)
        
    Returns:
        bool: True if file exists, False otherwise
        
    Example:
        # As PythonOperator callable
        check_task = PythonOperator(
            task_id='check_file',
            python_callable=check_file_exists,
            op_kwargs={'filepath': '/path/to/file', 'ssh_conn_id': 'my_ssh_conn'}
        )
        
        # Direct function call
        exists = check_file_exists('/path/to/file', 'my_ssh_conn')
    """
    try:
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            stdin, stdout, stderr = ssh_client.exec_command(f'test -f {filepath} && echo "EXISTS" || echo "NOT_EXISTS"')
            result = stdout.read().decode().strip()
            
            if result == "EXISTS":
                logging.info(f"File {filepath} exists on {ssh_conn_id}")
                return True
            else:
                logging.info(f"File {filepath} does not exist on {ssh_conn_id}")
                return False
                
    except Exception as e:
        logging.error(f"Error checking file {filepath} on {ssh_conn_id}: {str(e)}")
        return False


def check_file_exists_with_pattern(file_pattern: str, ssh_conn_id: str, **context) -> bool:
    """
    Check if files matching a pattern exist on remote server via SSH
    
    Args:
        file_pattern (str): File pattern to check (supports glob patterns)
        ssh_conn_id (str): SSH connection ID configured in Airflow
        **context: Airflow context (when used as PythonOperator callable)
        
    Returns:
        bool: True if any files matching pattern exist, False otherwise
        
    Example:
        # Check for files matching pattern
        exists = check_file_exists_with_pattern('/path/to/batchproc*', 'my_ssh_conn')
    """
    try:
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            # Use ls with pattern and count results
            stdin, stdout, stderr = ssh_client.exec_command(f'ls {file_pattern} 2>/dev/null | wc -l')
            result = stdout.read().decode().strip()
            count = int(result)
            
            if count > 0:
                logging.info(f"Found {count} files matching pattern {file_pattern} on {ssh_conn_id}")
                return True
            else:
                logging.info(f"No files matching pattern {file_pattern} on {ssh_conn_id}")
                return False
                
    except Exception as e:
        logging.error(f"Error checking file pattern {file_pattern} on {ssh_conn_id}: {str(e)}")
        return False


def check_directory_exists(dirpath: str, ssh_conn_id: str, **context) -> bool:
    """
    Check if a directory exists on remote server via SSH
    
    Args:
        dirpath (str): Remote directory path to check
        ssh_conn_id (str): SSH connection ID configured in Airflow
        **context: Airflow context (when used as PythonOperator callable)
        
    Returns:
        bool: True if directory exists, False otherwise
    """
    try:
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            stdin, stdout, stderr = ssh_client.exec_command(f'test -d {dirpath} && echo "EXISTS" || echo "NOT_EXISTS"')
            result = stdout.read().decode().strip()
            
            if result == "EXISTS":
                logging.info(f"Directory {dirpath} exists on {ssh_conn_id}")
                return True
            else:
                logging.info(f"Directory {dirpath} does not exist on {ssh_conn_id}")
                return False
                
    except Exception as e:
        logging.error(f"Error checking directory {dirpath} on {ssh_conn_id}: {str(e)}")
        return False


def get_file_size(filepath: str, ssh_conn_id: str, **context) -> Optional[int]:
    """
    Get file size on remote server via SSH
    
    Args:
        filepath (str): Remote file path
        ssh_conn_id (str): SSH connection ID configured in Airflow
        **context: Airflow context (when used as PythonOperator callable)
        
    Returns:
        Optional[int]: File size in bytes, None if file doesn't exist or error
    """
    try:
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            stdin, stdout, stderr = ssh_client.exec_command(f'stat -c%s {filepath} 2>/dev/null || echo "ERROR"')
            result = stdout.read().decode().strip()
            
            if result != "ERROR":
                size = int(result)
                logging.info(f"File {filepath} size: {size} bytes on {ssh_conn_id}")
                return size
            else:
                logging.warning(f"File {filepath} not found or error on {ssh_conn_id}")
                return None
                
    except Exception as e:
        logging.error(f"Error getting file size {filepath} on {ssh_conn_id}: {str(e)}")
        return None


def wait_for_file_stable(filepath: str, ssh_conn_id: str, stable_seconds: int = 60, **context) -> bool:
    """
    Wait for file to be stable (no size changes) for specified seconds
    
    Args:
        filepath (str): Remote file path to monitor
        ssh_conn_id (str): SSH connection ID configured in Airflow
        stable_seconds (int): Seconds to wait for stable file size
        **context: Airflow context (when used as PythonOperator callable)
        
    Returns:
        bool: True if file is stable, False if timeout or error
    """
    import time
    
    try:
        initial_size = get_file_size(filepath, ssh_conn_id)
        if initial_size is None:
            logging.error(f"File {filepath} not found for stability check")
            return False
            
        logging.info(f"Monitoring file {filepath} for {stable_seconds}s stability")
        time.sleep(stable_seconds)
        
        final_size = get_file_size(filepath, ssh_conn_id)
        if final_size is None:
            logging.error(f"File {filepath} disappeared during stability check")
            return False
            
        if initial_size == final_size:
            logging.info(f"File {filepath} is stable (size: {final_size} bytes)")
            return True
        else:
            logging.warning(f"File {filepath} size changed: {initial_size} -> {final_size}")
            return False
            
    except Exception as e:
        logging.error(f"Error during file stability check {filepath}: {str(e)}")
        return False


# SSH Connection ID constants for reuse across DAGs
class SSHConnections:
    """Constants for SSH connection IDs used across ALBA and SLRE DAGs"""
    TGEN_VL101 = 'tgen_vl101'  # Main Linux processing server
    TGEN_VL105 = 'tgen_vl105'  # File monitoring server
    TOPR_VW103 = 'topr_vw103'  # Windows batch processing server
