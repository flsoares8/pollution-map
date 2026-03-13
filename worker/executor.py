import logging

from tasks import feature_extraction, reduce_stage

logger = logging.getLogger(__name__)

TASK_HANDLERS = {
    "feature_extraction": feature_extraction.run,
    "reduce": reduce_stage.run,
}


def execute_task(task: dict) -> None:
    task_id = task["task_id"]
    task_type = task["type"]
    logger.info("Executing task %s (type: %s)", task_id, task_type)
    TASK_HANDLERS[task_type](task)
