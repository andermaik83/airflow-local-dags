"""
Airflow DAG Utilities Package

This package contains shared utility functions and constants for use across
multiple Airflow DAGs.

Modules:
- common_utils: SSH command generators and connection utilities

Usage:
    # Import command generators and connection constants
    from utils import check_file_exists, SSHConnections
    
    # Use with SSHOperator
    SSHOperator(
        task_id='check_file_exists',
        ssh_conn_id=SSHConnections.TGEN_VL105,
        command=check_file_exists('/TEST/SHR/ALBA/work/ALBA_imgissue.par')
    )
"""

# Export SSH command generators and utilities
from .common_utils import (
    check_file_exists,
    check_file_pattern,
    get_environment_from_path,
    check_directory,
    get_file_size,
    wait_for_stable_file,
    SSHConnections
)

# Export only the new command generator functions
__all__ = [,
    'check_file_exists',
    'check_file_pattern',
    'get_environment_from_path',
    'check_directory', 
    'get_file_size',
    'wait_for_stable_file',
    'SSHConnections'
]
