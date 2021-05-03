import os
import time
from celery import Celery
from celery.signals import task_received, task_prerun, task_postrun
from redis import Redis

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://redis:6379"),)
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379")

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
redis = Redis(host="redis", port=6379, db=0)

key_prefix_received_timestamp = "task_received_timestamp"
dict_prerun_timestamp = {}


@task_received.connect
def task_received_handler(sender=None, headers=None, body=None, request=None, **kwargs):
    redis.set(f"{key_prefix_received_timestamp}_{request.id}", time.time())
    

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
    except (KeyError, TypeError) as e:
        total_cost, run_cost = -1, -1
    print(f"{total_cost}, {run_cost}")


@celery.task(name="tasks.sleep")
def sleep(t: int):
    time.sleep(t)