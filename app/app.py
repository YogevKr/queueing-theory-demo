import time

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
key_producer_run = "producer_run"
key_number_of_workers = "number_of_workers"
key_restarted_flag = "restarted_flag"


def reset():
    redis.set(key_producer_run, "")
    celery.control.purge()
    redis.delete(key_total_time)
    redis.delete(key_run_time)
    redis.delete("celery")
    redis.set(key_restarted_flag, "True")
    redis.set(key_producer_run, "True")


reset()

st.sidebar.title("Queueing Theory Demo")
st.title("Simulation")


arrivals_distributaion = st.sidebar.selectbox(
    "Arrivals Distributaion", ("Fixed", "Poisson", "Normal")
)
redis.set(key_arrivals_distributaion, arrivals_distributaion)

arrivals_rate = st.sidebar.slider("Arrivals Rate (per minute)", 10, 60, 30)
redis.set(key_arrivals_rate, 1 / (arrivals_rate / 60))

departures_distributaion = st.sidebar.selectbox(
    "Departures Distributaion", ("Fixed", "Poisson", "Normal")
)
redis.set(key_departures_distributaion, departures_distributaion)

departures_rate = st.sidebar.slider("Departures Rate (per minute)", 10, 60, 30)
redis.set(key_departures_rate, 1 / (departures_rate / 60))

number_of_workers = st.sidebar.slider("Number of wokrers", 1, 10, 1)
redis.set(key_number_of_workers, number_of_workers)

number_of_tasks = st.sidebar.slider("Number of tasks", 10, 1000, 20, 10)

if st.sidebar.button("Reset"):
    reset()

redis.set(key_producer_run, "True")

progress_bar = st.progress(0)
status_text = st.empty()

chart = st.line_chart(pd.DataFrame([], columns=["Total time", "Service time"]))

counter = 0

while counter < number_of_tasks:
    number_of_reported_tasks = redis.llen(key_total_time)
    while counter < number_of_reported_tasks:
        total_time = redis.lindex(key_total_time, counter) or 0
        rum_time = redis.lindex(key_run_time, counter) or 0
        chart.add_rows(
            pd.DataFrame(
                [[float(total_time), float(rum_time)]],
                columns=["Total time", "Service time"],
            )
        )
        counter += 1
    status_text.text(f"{counter} Complete")

    progress_bar.progress(counter / number_of_tasks)
    time.sleep(0.1)

redis.set(key_producer_run, "")

progress_bar.empty()
