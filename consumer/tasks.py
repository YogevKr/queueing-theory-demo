import os
import time

from celery import Celery
from celery.signals import task_postrun, task_prerun, task_received
from redis import Redis

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://redis:6379"),)
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379")

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
celery.conf.worker_prefetch_multiplier = 1

redis = Redis(host="redis", port=6379, db=0)

key_prefix_received_timestamp = "task_received_timestamp"
key_total_time = "total_time_tasks"
key_run_time = "run_time_tasks"
dict_prerun_timestamp = {}


@task_prerun.connect
def task_prerun_handler(signal, sender, task_id, task, args, kwargs, **extras):
    dict_prerun_timestamp[task_id] = time.time()


@task_postrun.connect
def task_postrun_handler(
    signal, sender, task_id, task, args, kwargs, retval, state, **extras
):
    try:
        t = time.time()
        total_cost = t - float(redis.get(f"{key_prefix_received_timestamp}_{task_id}"))
        run_cost = t - dict_prerun_timestamp.pop(task_id)

        print(f"Total cost - {total_cost}")

        redis.rpush(key_total_time, total_cost)
        redis.rpush(key_run_time, run_cost)
    except (KeyError, TypeError) as e:
        print(e, task_id)


@celery.task(name="tasks.sleep")
def sleep(t: float):
    time.sleep(t)
