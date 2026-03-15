import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")


def extract_features(record: dict) -> dict:
    clicks = record["clicks"]
    session_time = record["session_time"]
    page_views = record["page_views"]
    purchases = record["purchases"]

    return {
        "user_id": record["user_id"],
        "session_id": record["session_id"],
        "features": {
            "click_rate": round(clicks / session_time, 4) if session_time > 0 else 0,
            "engagement_score": round(page_views / session_time, 4) if session_time > 0 else 0,
            "conversion": 1 if purchases > 0 else 0,
        },
    }


def run(task: dict) -> None:
    task_id = task["task_id"]
    chunk_path = task["chunk_path"]

    records = json.loads(Path(chunk_path).read_text())
    results = [extract_features(record) for record in records]

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{task_id}.json"
    output_path.write_text(json.dumps(results, indent=2))

    logger.info("Task %s: extracted features for %d records -> %s", task_id, len(results), output_path)
