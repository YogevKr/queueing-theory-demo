import numpy as np
import pandas as pd
import streamlit as st
from worker import celery
import matplotlib.pyplot as plt
import time
import numpy as np
from collections import deque

st.title("Simple Streamlit App")

if st.button("Say hello"):
    st.write(celery.send_task("tasks.sleep", args=[5], kwargs={}))

progress_bar = st.sidebar.progress(0)
status_text = st.sidebar.empty()
last_rows = np.random.randn(1, 1)
chart = st.line_chart(last_rows)

for i in range(1, 101):
    new_rows = last_rows[-1, :] + np.random.randn(5, 1).cumsum(axis=0)
    new_rows = np.random.randn(1, 1)
    status_text.text("%i%% Complete" % i)
    chart.add_rows(new_rows)
    progress_bar.progress(i)
    last_rows = new_rows
    time.sleep(0.05)

progress_bar.empty()

# Streamlit widgets automatically run the script from top to bottom. Since
# this button is not connected to any other logic, it just causes a plain
# rerun.
st.button("Re-run")
