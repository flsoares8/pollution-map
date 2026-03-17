import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")

PM25_SAFE_LIMIT = 75.0
NO2_SAFE_LIMIT = 200.0


def compute_air_quality_features(record: dict) -> dict:
    pm25 = record["pm25"]
    co2 = record["co2"]
    no2 = record["no2"]

    air_quality_index = round((pm25 * 0.5) + (no2 * 0.3) + (co2 / 400 * 20), 2) # Not sure about the formula. TODO: Revisit it
    exceeds_safe_limit = 1 if pm25 > PM25_SAFE_LIMIT or no2 > NO2_SAFE_LIMIT else 0

    if air_quality_index < 50:
        pollution_level = "low"
    elif air_quality_index < 100:
        pollution_level = "moderate"
    else:
        pollution_level = "high"

    return {
        "sensor_id": record["sensor_id"],
        "location": record["location"],
        "features": {
            "aqi": air_quality_index,
            "exceeds_safe_limit": exceeds_safe_limit,
            "pollution_level": pollution_level,
        },
    }


def run(task: dict) -> None:
    task_id = task["task_id"]
    chunk_path = task["chunk_path"]

    records = json.loads(Path(chunk_path).read_text())
    results = [compute_air_quality_features(record) for record in records]

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{task_id}.json"
    output_path.write_text(json.dumps(results, indent=2))

    logger.info("Task %s: extracted features for %d records -> %s", task_id, len(results), output_path)
