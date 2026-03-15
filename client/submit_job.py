import argparse
import logging
import os

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

SCHEDULER_URL = os.getenv("SCHEDULER_URL", "http://localhost:8000")


def submit_job(dataset_path: str, chunk_size: int) -> dict:
    response = requests.post(
        f"{SCHEDULER_URL}/submit_job",
        json={"dataset_path": dataset_path, "chunk_size": chunk_size},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit a ChunkFlow processing job")
    parser.add_argument("dataset_path", help="Path to the dataset JSON file")
    parser.add_argument("chunk_size", type=int, help="Number of records per chunk")
    args = parser.parse_args()

    result = submit_job(args.dataset_path, args.chunk_size)
    logger.info("Job submitted: %s", result)
