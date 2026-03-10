import json
import logging
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


def partition_dataset(file_path: str, chunk_size: int) -> list[str]:
    """Split a JSON dataset file into chunk files on disk.

    Args:
        file_path: Path to a JSON file containing a list of records.
        chunk_size: Number of records per chunk.

    Returns:
        List of paths to the written chunk files.
    """
    source = Path(file_path)
    records = json.loads(source.read_text())

    output_dir = source.parent
    stem = source.stem
    chunk_files = []

    for i, offset in enumerate(range(0, len(records), chunk_size), start=1):
        chunk = records[offset : offset + chunk_size]
        chunk_path = output_dir / f"{stem}_chunk_{i}.json"
        chunk_path.write_text(json.dumps(chunk, indent=2))
        chunk_files.append(str(chunk_path))
        logger.info("Written chunk %d with %d records to %s", i, len(chunk), chunk_path)

    logger.info("Partitioned %d records into %d chunks", len(records), len(chunk_files))
    return chunk_files


def create_tasks(job_id: str, chunk_files: list[str]) -> list[dict]:
    tasks = [
        {"task_id": str(uuid.uuid4()), "job_id": job_id, "chunk_path": chunk_path}
        for chunk_path in chunk_files
    ]
    logger.info("Created %d tasks for job %s", len(tasks), job_id)
    return tasks
