import logging

from tasks import feature_extraction

logger = logging.getLogger(__name__)


def execute_task(task: dict) -> None:
    task_id = task["task_id"]
    logger.info("Executing task %s", task_id)
    feature_extraction.run(task)
