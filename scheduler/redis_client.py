import json
import logging
import time
from typing import Optional

import redis

from scheduler.config import config

logger = logging.getLogger(__name__)

TASK_QUEUE_KEY = "task_queue"
RUNNING_TASKS_KEY = "running_tasks"
COMPLETED_TASKS_KEY = "completed_tasks"
WORKERS_KEY = "workers"
WORKER_ACTIVE_TTL = 15  # seconds without heartbeat before considered inactive


def get_client() -> redis.Redis:
    return redis.Redis.from_url(config.redis_url, decode_responses=True)


def enqueue_task(task: dict) -> None:
    client = get_client()
    client.lpush(TASK_QUEUE_KEY, json.dumps(task))
    logger.info("Enqueued task %s", task.get("task_id"))


def dequeue_task() -> Optional[dict]:
    client = get_client()
    raw = client.rpop(TASK_QUEUE_KEY)
    if raw is None:
        return None
    task = json.loads(raw)
    logger.info("Dequeued task %s", task.get("task_id"))
    return task


def mark_task_running(task_id: str) -> None:
    client = get_client()
    client.sadd(RUNNING_TASKS_KEY, task_id)
    logger.info("Task %s marked as running", task_id)


def mark_task_complete(task_id: str) -> None:
    client = get_client()
    client.srem(RUNNING_TASKS_KEY, task_id)
    client.sadd(COMPLETED_TASKS_KEY, task_id)
    logger.info("Task %s marked as complete", task_id)


def register_job(job_id: str, task_ids: list[str]) -> None:
    client = get_client()
    pipe = client.pipeline()
    for task_id in task_ids:
        pipe.sadd(f"job:{job_id}:tasks", task_id)
        pipe.set(f"task:{task_id}:job", job_id)
    pipe.execute()
    logger.info("Registered job %s with %d tasks", job_id, len(task_ids))


def get_task_job_id(task_id: str) -> Optional[str]:
    client = get_client()
    return client.get(f"task:{task_id}:job")


def get_job_task_ids(job_id: str) -> list[str]:
    client = get_client()
    return list(client.smembers(f"job:{job_id}:tasks"))


def update_worker_heartbeat(worker_id: str, timestamp: float) -> None:
    client = get_client()
    client.zadd(WORKERS_KEY, {worker_id: timestamp})
    logger.debug("Heartbeat received from worker %s", worker_id)


def get_metrics() -> dict:
    client = get_client()
    active_since = time.time() - WORKER_ACTIVE_TTL
    active_workers = client.zcount(WORKERS_KEY, active_since, "+inf")
    tasks_pending = client.llen(TASK_QUEUE_KEY)
    tasks_running = client.scard(RUNNING_TASKS_KEY)
    return {
        "active_workers": active_workers,
        "tasks_pending": tasks_pending,
        "tasks_running": tasks_running,
    }


def all_tasks_complete(job_id: str) -> bool:
    client = get_client()
    task_ids = client.smembers(f"job:{job_id}:tasks")
    if not task_ids:
        return False
    completed = client.smembers(COMPLETED_TASKS_KEY)
    return task_ids.issubset(completed)
