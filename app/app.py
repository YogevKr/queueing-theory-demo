import time
from collections import deque

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
from redis import Redis

from worker import celery

redis = Redis(host="redis", port=6379, db=0)

key_total_time = "total_time_tasks"
key_run_time = "run_time_tasks"

key_arrivals_rate = "arrivals-rate"
key_departures_rate = "departures-rate"
key_arrivals_distributaion = "arrivals-distributaion"
key_departures_distributaion = "departures-distributaion"
key_on_off = "producer_run"


def reset():
    redis.delete(key_total_time)
    redis.delete(key_run_time)
    redis.delete("celery")
    celery.control.purge()


reset()

st.title("Queueing Theory Demo")

if st.button("Reset"):
    reset()

arrivals_rate = st.slider("Arrivals Rate", 2, 10, 2)
redis.set(key_arrivals_rate, arrivals_rate)

departures_rate = st.slider("Departures Rate", 2, 10, 2)
redis.set(key_departures_rate, departures_rate)


progress_bar = st.sidebar.progress(0)
status_text = st.sidebar.empty()

chart = st.line_chart(pd.DataFrame([[0.0, 0.0]], columns=["Total time", "Run time"]))

counter = 0

while True:

    number_of_repordet_tasks = redis.llen(key_total_time)
    while counter < number_of_repordet_tasks:
        total_time = redis.lindex(key_total_time, counter) or 0
        rum_time = redis.lindex(key_run_time, counter) or 0
        chart.add_rows(
            pd.DataFrame(
                [[float(total_time), float(rum_time)]],
                columns=["Total time", "Run time"],
            )
        )
        counter += 1
    # status_text.text("%i%% Complete" % i)

    # progress_bar.progress(i)
    time.sleep(0.1)

progress_bar.empty()
