"""Shared bits for the GitHub Open Source Pulse DAGs (PLAN.md §8)."""
import os
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator

PROJECT = os.environ.get("PROJECT_DIR", "/opt/pulse")

DEFAULT_ARGS = {
    "owner": "pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=8),
}


def pulse(task_id, args, **kw):
    """Run a pipeline step in the edge container: the same command make and the viva use."""
    return BashOperator(task_id=task_id, bash_command=f"docker exec bda-edge pulse {args}", **kw)


def stage(name, task_id=None):
    """Switch the running service set (memory budget) before a group of steps; idempotent."""
    return BashOperator(task_id=task_id or f"stage_{name}", cwd=PROJECT,
                        bash_command=f"scripts/stage.sh {name}", retries=1)
