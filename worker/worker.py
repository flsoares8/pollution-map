import logging
import os
import time
import uuid

import requests

from worker.executor import execute_task
from worker.heartbeat import start as start_heartbeat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

SCHEDULER_URL = os.getenv("SCHEDULER_URL", "http://localhost:8000")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))


def run() -> None:
    worker_id = str(uuid.uuid4())
    logger.info("Worker started with id %s", worker_id)
    start_heartbeat(worker_id, SCHEDULER_URL)
    while True:
        response = requests.get(f"{SCHEDULER_URL}/task")
        response.raise_for_status()
        task = response.json().get("task")

        if task is None:
            time.sleep(POLL_INTERVAL)
            continue

        task_id = task["task_id"]
        logger.info("Task received: %s", task_id)

        execute_task(task)

        requests.post(f"{SCHEDULER_URL}/task/{task_id}/complete").raise_for_status()
        logger.info("Task completed: %s", task_id)


if __name__ == "__main__":
    run()
