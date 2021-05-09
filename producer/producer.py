import logging
import os
import sys
import time

import numpy as np
from celery import Celery
from celery.app.control import Control
from redis import Redis

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger(__name__)

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://redis:6379"),)
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379")

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
controller = Control(celery)
redis = Redis(host="redis", port=6379, db=0, charset="utf-8", decode_responses=True)

key_prefix_received_timestamp = "task_received_timestamp"
key_arrivals_rate = "arrivals-rate"
key_departures_rate = "departures-rate"
key_arrivals_distributaion = "arrivals-distributaion"
key_departures_distributaion = "departures-distributaion"
key_producer_run = "producer_run"
key_number_of_workers = "number_of_workers"
key_restarted_flag = "restarted_flag"


def sample_from_distributaion(scale: float, type: str = None) -> float:
    if not type or type.lower() == "deterministic":
        return scale
    elif type.lower() == "poisson":
        return np.random.exponential(scale=scale)
    elif type.lower() == "normal":
        v = np.random.normal(scale=scale)
        return v if v > 0 else 0
    else:
        logger.info(type)
        return scale


def scale_workers(current_number_of_workers: int, expected_number_of_workers: int):
    if expected_number_of_workers > current_number_of_workers:
        controller.pool_grow(expected_number_of_workers - current_number_of_workers)
        logger.info(
            f"pool_grow: {expected_number_of_workers - current_number_of_workers}"
        )
    elif expected_number_of_workers < current_number_of_workers:
        controller.pool_shrink(1)
        logger.info(
            f"pool_shrink: {current_number_of_workers - expected_number_of_workers}"
        )


def main():
    logger.info("Starting..")
    current_number_of_workers = 1

    while True:
        expected_number_of_workers = int(redis.get(key_number_of_workers) or 1)
        if expected_number_of_workers != current_number_of_workers:
            current_number_of_workers = scale_workers(
                current_number_of_workers, expected_number_of_workers
            )

            stats = celery.control.inspect().stats()
            if stats:
                current_number_of_workers = list(stats.values())[0]["prefetch_count"]
            else:
                logger.warning("No celery stats fetched")
            logger.info(current_number_of_workers)
        if redis.get(key_producer_run):
            redis.delete(key_restarted_flag)
            arrivals_rate = redis.get(key_arrivals_rate) or 1
            departures_rate = redis.get(key_departures_rate) or 1
            arrivals_distributaion = redis.get(key_arrivals_distributaion)
            departures_distributaion = redis.get(key_departures_distributaion)

            arrival_time = sample_from_distributaion(
                float(arrivals_rate), arrivals_distributaion
            )
            departures_time = sample_from_distributaion(
                float(departures_rate), departures_distributaion
            )
            logger.info(
                f"arrival_time: {arrival_time}, departures_time: {departures_time}"
            )

            time.sleep(arrival_time)
            if not redis.get(key_restarted_flag):
                task_id = celery.send_task(
                    "tasks.sleep", args=[departures_time], kwargs={}
                )
                redis.set(f"{key_prefix_received_timestamp}_{task_id}", time.time())
        else:
            time.sleep(0.1)


if __name__ == "__main__":
    main()
