import statistics
import time
from collections import Counter

import pandas as pd
import streamlit as st
from redis import Redis
from worker import celery

inspector = celery.control.inspect()
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


reset()

st.sidebar.title("Queueing Theory Demo")


arrivals_distributaion = st.sidebar.selectbox(
    "Arrivals Distributaion", ("Poisson", "Deterministic", "Normal")
)
arrivals_rate = st.sidebar.slider("Arrivals Rate (per minute)", 10, 60, 30)
departures_distributaion = st.sidebar.selectbox(
    "Departures Distributaion", ("Poisson", "Deterministic", "Normal")
)
departures_rate = st.sidebar.slider("Departures Rate (per minute)", 10, 60, 30)

number_of_workers = st.sidebar.slider("Number of wokrers", 1, 10, 1)

number_of_tasks = st.sidebar.slider("Number of tasks", 10, 1000, 100, 10)

queue_types = {"Deterministic": "D", "Poisson": "M", "Normal": "N"}

st.header("Parameters")
st.markdown(
    f"Queue type: {queue_types[arrivals_distributaion]}/{queue_types[departures_distributaion]}/{number_of_workers}"
)
st.markdown(f"$\lambda={arrivals_rate}, \mu={departures_rate}$")

start_simulation = False
auto_start = st.sidebar.checkbox("Auto start", True)
if not auto_start and st.sidebar.button("Start"):
    start_simulation = True

if auto_start or start_simulation:
    number_of_tasks_in_the_queue = []
    if st.sidebar.button("Reset"):
        reset()
    reset()
    redis.set(key_arrivals_distributaion, arrivals_distributaion)
    redis.set(key_arrivals_rate, 1 / (arrivals_rate / 60))
    redis.set(key_departures_distributaion, departures_distributaion)
    redis.set(key_departures_rate, 1 / (departures_rate / 60))
    redis.set(key_number_of_workers, number_of_workers)
    redis.set(key_producer_run, "True")

    progress_bar = st.progress(0)
    status_text = st.empty()

    chart = st.line_chart(pd.DataFrame([], columns=["Total time", "Service time"]))

    W_ = st.empty()
    L_ = st.empty()

    subtitle = st.empty()
    steady_state_probabilities = st.empty()

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
            l_q = redis.llen("celery")
            active_ = inspector.active()
            l_service = len(list(active_.values())[0]) if active_ else 0
            number_of_tasks_in_the_queue.append(l_q + l_service)
            counter += 1
            W_average_waiting_time = statistics.mean(
                map(float, redis.lrange(key_total_time, 0, -1))
            )
            W_.markdown(
                f"$W$ (Average time in the system): {W_average_waiting_time:.2f} Seconds"
            )
            L_average_waiting_time = statistics.mean(number_of_tasks_in_the_queue)
            L_.markdown(
                f"$L$ (Average customers in the system): {L_average_waiting_time:.2f}"
            )

            subtitle.markdown("**Obsereved Steady State Probabilities**")
            df = pd.DataFrame.from_dict(
                Counter(number_of_tasks_in_the_queue),
                orient="index",
                columns=["Number of customers"],
            )
            steady_state_probabilities.bar_chart(df / df["Number of customers"].sum())

            status_text.text(f"{counter} Complete")
            progress_bar.progress(counter / number_of_tasks)

        time.sleep(0.1)

    redis.set(key_producer_run, "")

    progress_bar.empty()
