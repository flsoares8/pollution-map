import logging
import os
import threading
import time

import requests

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "5.0"))


def _send_heartbeats(worker_id: str, scheduler_url: str) -> None:
    while True:
        try:
            requests.post(
                f"{scheduler_url}/heartbeat",
                json={"worker_id": worker_id, "timestamp": time.time()},
                timeout=5,
            ).raise_for_status()
            logger.debug("Heartbeat sent for worker %s", worker_id)
        except Exception as e:
            logger.warning("Failed to send heartbeat: %s", e)
        time.sleep(HEARTBEAT_INTERVAL)


def start(worker_id: str, scheduler_url: str) -> None:
    thread = threading.Thread(target=_send_heartbeats, args=(worker_id, scheduler_url), daemon=True)
    thread.start()
    logger.info("Heartbeat thread started for worker %s", worker_id)
