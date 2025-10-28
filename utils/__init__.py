"""
Airflow DAG Utilities Package

This package contains shared utility functions and constants for use across
multiple Airflow DAGs.

Modules:
- common_utils: SSH connection and file checking utilities

Usage:
    # Import core functions and connection constants
    from utils import check_file_exists, SSHConnections
    
    # Define application-specific file paths in your DAG
    ALBA_IMG_ISSUE_FILE = "/TEST/SHR/ALBA/work/ALBA_imgissue.par"
"""

# Export commonly used SSH utilities
from .common_utils import (
    check_file_exists,
    check_file_exists_with_pattern,
    check_directory_exists,
    get_file_size,
    wait_for_file_stable,
    get_environment_from_path
)

# Export only core utilities that most DAGs will use
__all__ = [
    'check_file_exists',
    'check_file_exists_with_pattern', 
    'check_directory_exists',
    'get_file_size',
    'wait_for_file_stable',
    'get_environment_from_path'
]
