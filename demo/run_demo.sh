#!/bin/bash
set -e

SCHEDULER_URL="http://localhost:8000"
DATASET_PATH="dataset/sample_dataset.json"
CHUNK_SIZE=10
WORKERS=3

echo "========================================"
echo "       ChunkFlow Demo"
echo "========================================"

echo "Cleaning up any previous state..."
docker compose down --volumes 2>/dev/null || true

echo "Starting ChunkFlow services (scheduler, redis, $WORKERS workers)..."
docker compose up -d --scale worker=$WORKERS

echo "Waiting for scheduler to be ready..."
until curl -sf "$SCHEDULER_URL/health" > /dev/null; do
    sleep 1
done
echo "Scheduler is ready."

echo "Submitting dataset job..."
echo "  Dataset : $DATASET_PATH"
echo "  Chunk size: $CHUNK_SIZE records per chunk"
RESPONSE=$(curl -sf -X POST "$SCHEDULER_URL/submit_job" \
    -H "Content-Type: application/json" \
    -d "{\"dataset_path\": \"$DATASET_PATH\", \"chunk_size\": $CHUNK_SIZE}")
JOB_ID=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['job_id'])")
TASKS=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['tasks_created'])")
echo "  Job ID    : $JOB_ID"
echo "  Tasks created: $TASKS"

echo "Workers are processing dataset chunks..."
echo "  Polling metrics every 2 seconds. Waiting for all tasks to complete..."

while true; do
    METRICS=$(curl -sf "$SCHEDULER_URL/metrics")
    PENDING=$(echo "$METRICS" | python3 -c "import sys, json; print(json.load(sys.stdin)['tasks_pending'])")
    RUNNING=$(echo "$METRICS" | python3 -c "import sys, json; print(json.load(sys.stdin)['tasks_running'])")
    ACTIVE_WORKERS=$(echo "$METRICS" | python3 -c "import sys, json; print(json.load(sys.stdin)['active_workers'])")
    echo "  Active workers: $ACTIVE_WORKERS | Tasks pending: $PENDING | Tasks running: $RUNNING"
    if [ "$PENDING" -eq 0 ] && [ "$RUNNING" -eq 0 ]; then
        break
    fi
    sleep 2
done

echo "All tasks complete. Final dataset written to output/final_features_dataset.json"

echo "Preview of results:"
python3 -c "
import json
records = json.load(open('output/final_features_dataset.json'))
for record in records:
    print(' ', json.dumps(record))
"
echo "Shutting down services..."
docker compose down
