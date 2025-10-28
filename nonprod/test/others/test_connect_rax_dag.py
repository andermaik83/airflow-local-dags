# test/ssh_run_hari.py
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

DAG_ID = "test_connect_rax_dag"
REMOTE_DIR = "/TEST/SHR/USERS/data/appcmatomcatt"
SCRIPT_NAME = "hari.sh"
SSH_CONN_ID = "RAXTest"

default_args = {
    "owner": "airflow",
    # TODO: Replace with your team distribution list if desired
    "email": ["harikrishna.savarala@clarivate.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # set >0 if you want retries
}

with DAG(
        dag_id=DAG_ID,
        description="Run hari.sh on remote host via SSH using RAXTest connection",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=None,           # Manual only
        catchup=False,
        default_args=default_args,
        tags=["ssh", "ops", "cm"],
        doc_md=f"""
**What it does**

1. **Pre-checks** on remote host:
   - Verifies directory `{REMOTE_DIR}` exists
   - Verifies `{SCRIPT_NAME}` exists and is executable

2. **Runs** `{SCRIPT_NAME}` in `{REMOTE_DIR}`

**Connection required**  
Airflow connection `ssh://{SSH_CONN_ID}` (type: *SSH*) with hostname, username, and auth (password or private key).
    """,
) as dag:

    # Pre-flight: ensure directory & script exist; make script executable; fail fast if anything is wrong
    precheck_remote = SSHOperator(
        task_id="precheck_remote",
        ssh_conn_id=SSH_CONN_ID,
        get_pty=True,               # better log behavior over SSH
        cmd_timeout=600,            # time allowed for the remote command (seconds)
        # strict mode + explicit checks; any failure = non-zero exit -> task fails
        command=(
            "bash -lc '"
            "set -Eeuo pipefail; set -x; "
            f"test -d {REMOTE_DIR} "
            f"&& cd {REMOTE_DIR} "
            f"&& test -f {SCRIPT_NAME} "
            f"&& chmod +x {SCRIPT_NAME} "
            f"&& test -x {SCRIPT_NAME} "
            "&& echo \"[precheck] OK: directory & script are present and executable\""
            "'"
        ),
        do_xcom_push=False,
    )

    # Execute the script
    run_hari_script = SSHOperator(
        task_id="run_hari_script",
        ssh_conn_id=SSH_CONN_ID,
        get_pty=True,
        cmd_timeout=1800,           # 30 min; adjust if needed
        command=(
            "bash -lc '"
            "set -Eeuo pipefail; set -x; "
            f"cd {REMOTE_DIR} "
            f"&& ./{SCRIPT_NAME}"
            "'"
        ),
        do_xcom_push=False,
    )

    precheck_remote >> run_hari_script
