import os
import time
from celery import Celery
from redis import Redis
import numpy as np

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://redis:6379"),)
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379")

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
redis = Redis(host="redis", port=6379, db=0)

key_arrivals_rate = "arrivals-rate"
key_departures_rate = "departures-rate"
key_arrivals_distributaion = "arrivals-distributaion"
key_departures_distributaion = "departures-distributaion"
key_on_off = "producer_run"


def main():
    print("Starting..")
    while True:
        arrivals_rate = redis.get(key_arrivals_rate) or 1
        departures_rate = redis.get(key_departures_rate) or 1

        # time.sleep(np.random.exponential(float(arrivals_rate)))
        # celery.send_task("tasks.sleep", args=[np.random.exponential(float(departures_rate))], kwargs={})
        time.sleep(float(arrivals_rate))
        celery.send_task("tasks.sleep", args=[float(departures_rate)], kwargs={})


if __name__ == "__main__":
    main()