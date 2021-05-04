import os
import time

import numpy as np
from celery import Celery
from redis import Redis

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://redis:6379"),)
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379")

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
redis = Redis(host="redis", port=6379, db=0, charset="utf-8", decode_responses=True)

key_arrivals_rate = "arrivals-rate"
key_departures_rate = "departures-rate"
key_arrivals_distributaion = "arrivals-distributaion"
key_departures_distributaion = "departures-distributaion"
key_producer_run = "producer_run"


def sample_from_disrebution(scale: float, type: str = None) -> float:
    if not type or type == "None":
        return scale
    elif type.lower() == "exponential":
        return np.random.exponential(scale=scale)
    elif type.lower() == "normal":
        v = np.random.normal(scale=scale)
        return v if v > 0 else 0
    else:
        print(type)
        return scale


def main():
    print("Starting..")
    while True:
        if redis.get(key_producer_run):
            arrivals_rate = redis.get(key_arrivals_rate) or 1
            departures_rate = redis.get(key_departures_rate) or 1
            arrivals_distributaion = redis.get(key_arrivals_distributaion)
            departures_distributaion = redis.get(key_departures_distributaion)

            arrival_time = sample_from_disrebution(
                float(arrivals_rate), arrivals_distributaion
            )
            departures_time = sample_from_disrebution(
                float(departures_rate), departures_distributaion
            )
            print(arrival_time, departures_time)
            time.sleep(arrival_time)
            celery.send_task("tasks.sleep", args=[departures_time], kwargs={})


if __name__ == "__main__":
    main()
