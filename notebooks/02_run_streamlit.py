#!/usr/bin/env python
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Run Streamlit App (Community Edition)
# MAGIC
# MAGIC This notebook launches the ACDOCA Synthetic Generator Streamlit app on the
# MAGIC cluster driver node and exposes it via the Databricks driver proxy.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Add this repo via **Workspace → Repos → Add Repo** using `https://github.com/GDAVIDREEVES/databricks-sap-synthetic-data.git`
# MAGIC 2. Attach this notebook to a running cluster.

# COMMAND ----------
# Install the project from the Repos checkout and streamlit
%pip install -e /Workspace/Repos/gdreeves@gmail.com/databricks-sap-synthetic-data streamlit
dbutils.library.restartPython()  # type: ignore[name-defined]

# COMMAND ----------
import subprocess
import os
import time

# Get cluster and org IDs for the proxy URL
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[name-defined]
cluster_id = ctx.clusterId().get()
workspace_url = ctx.apiUrl().get()

port = 8501

# Launch Streamlit as a background process
env = os.environ.copy()
env["STREAMLIT_SERVER_PORT"] = str(port)
env["STREAMLIT_SERVER_ADDRESS"] = "0.0.0.0"
env["STREAMLIT_SERVER_HEADLESS"] = "true"

# Find the installed app.py location
import acdoca_generator
app_path = os.path.join(os.path.dirname(acdoca_generator.__file__), "app.py")

proc = subprocess.Popen(
    ["streamlit", "run", app_path, "--server.port", str(port),
     "--server.address", "0.0.0.0", "--server.headless", "true"],
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)

# Give it a moment to start
time.sleep(5)

if proc.poll() is not None:
    print("Streamlit failed to start!")
    print(proc.stderr.read().decode())
else:
    proxy_url = f"{workspace_url}/driver-proxy/o/0/{cluster_id}/{port}/"
    print(f"Streamlit is running!")
    print(f"\nOpen the app at:\n{proxy_url}")
    displayHTML(f'<a href="{proxy_url}" target="_blank">Open Streamlit App</a>')  # type: ignore[name-defined]

# COMMAND ----------
# MAGIC %md
# MAGIC **Note:** The app runs as long as this notebook's cluster is active.
# MAGIC To stop it, detach the notebook or terminate the cluster.
