import logging

logger = logging.getLogger(__name__)


def execute_task(task: dict) -> None:
    task_id = task["task_id"]
    logger.info("Executing task %s", task_id)
    print(f"Executing task {task_id}")
