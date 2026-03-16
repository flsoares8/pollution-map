# ChunkFlow

ChunkFlow is a distributed dataset processing pipeline designed to execute feature extraction workloads across multiple worker nodes. It demonstrates core distributed systems concepts including task scheduling, worker coordination, chunk-based data partitioning, and result aggregation.

The system follows a simple scheduler-worker architecture inspired by concepts used in distributed computing frameworks such as Apache Spark and Ray, using Redis as a coordination layer for task distribution and state tracking.

## Architecture

See [Architecture](docs/architecture.md) for the full system diagram and component descriptions.

## Key Concepts Demonstrated

- **Distributed task scheduling** — a central scheduler partitions datasets and coordinates work across multiple workers
- **Worker coordination** — stateless workers poll for tasks, execute independently, and report results back
- **Fault tolerance** — worker heartbeats allow the scheduler to detect and monitor active nodes
- **Result aggregation** — a dedicated reduce stage merges intermediate outputs into a final dataset
- **Observability** — metrics endpoint exposes active workers, pending tasks, and running tasks in real time

## Tech Stack

- **Scheduler**: Python + FastAPI
- **Workers**: Python
- **Task coordination**: Redis
- **Communication**: HTTP REST
- **Containerization**: Docker

## Demo

To run the full pipeline end-to-end with a single command:

```bash
bash demo/run_demo.sh
```

This starts all services, submits a sample job, polls until completion, prints the results, and shuts everything down.

## Running Locally

**Start all services with 3 workers:**
```bash
docker compose up --scale worker=3
```

**Submit a job:**
```bash
PYTHONPATH=. python -m client.submit_job dataset/sample_dataset.json 3
```

**Check metrics:**
```bash
curl http://localhost:8000/metrics
```

## Status

Feature extraction pipeline fully operational. C++ compute extension planned.
