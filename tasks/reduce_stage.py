import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")


def run(task: dict) -> None:
    task_ids = task["task_ids"]
    output_dir = Path(OUTPUT_DIR)
    merged = []

    for task_id in task_ids:
        path = output_dir / f"{task_id}.json"
        records = json.loads(path.read_text())
        merged.extend(records)
        logger.info("Reduce: loaded %d records from %s", len(records), path)

    merged.sort(key=lambda r: r["user_id"])

    final_path = output_dir / "final_features_dataset.json"
    final_path.write_text(json.dumps(merged, indent=2))
    logger.info("Reduce complete: %d total records written to %s", len(merged), final_path)
