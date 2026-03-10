import logging
import uuid

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from scheduler.config import config
from scheduler.job_manager import create_tasks, partition_dataset
from scheduler.redis_client import enqueue_task

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

    return {"job_id": job_id, "tasks_created": len(tasks)}


@app.get("/health")
def health() -> dict:
    logger.info("Health check requested")
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("scheduler.main:app", host=config.host, port=config.port, reload=False)
