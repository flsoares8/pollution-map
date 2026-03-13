import logging
import uuid

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from scheduler.config import config
from scheduler.job_manager import create_tasks, partition_dataset
from scheduler.redis_client import (
    all_tasks_complete,
    dequeue_task,
    enqueue_task,
    get_job_task_ids,
    get_task_job_id,
    mark_task_complete,
    mark_task_running,
    register_job,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="ChunkFlow Scheduler")


class JobRequest(BaseModel):
    dataset_path: str
    chunk_size: int


@app.post("/submit_job")
def submit_job(request: JobRequest) -> dict:
    job_id = str(uuid.uuid4())
    logger.info("Submitting job %s for dataset %s", job_id, request.dataset_path)

    chunk_files = partition_dataset(request.dataset_path, request.chunk_size)
    tasks = create_tasks(job_id, chunk_files)

    for task in tasks:
        enqueue_task(task)

    register_job(job_id, [t["task_id"] for t in tasks])

    return {"job_id": job_id, "tasks_created": len(tasks)}


@app.get("/task")
def get_task() -> dict:
    task = dequeue_task()
    if task is None:
        return {"task": None}
    mark_task_running(task["task_id"])
    logger.info("Assigned task %s to worker", task["task_id"])
    return {"task": task}


@app.post("/task/{task_id}/complete")
def complete_task(task_id: str) -> dict:
    mark_task_complete(task_id)
    logger.info("Task %s reported complete", task_id)

    job_id = get_task_job_id(task_id)
    if job_id and all_tasks_complete(job_id):
        logger.info("All tasks complete for job %s, enqueueing reduce task", job_id)
        task_ids = get_job_task_ids(job_id)
        reduce_task = {
            "task_id": str(uuid.uuid4()),
            "job_id": job_id,
            "type": "reduce",
            "task_ids": task_ids,
        }
        enqueue_task(reduce_task)

    return {"status": "ok"}


@app.get("/health")
def health() -> dict:
    logger.info("Health check requested")
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("scheduler.main:app", host=config.host, port=config.port, reload=False)
