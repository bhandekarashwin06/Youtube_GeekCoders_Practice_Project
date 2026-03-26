# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE CONNECTION IF NOT EXISTS youtube_earthquake_conn2
# MAGIC type HTTP
# MAGIC OPTIONS (
# MAGIC   host = 'https://earthquake.usgs.gov',
# MAGIC   port = 443,
# MAGIC   base_path = '/earthquakes/feed/v1.0',
# MAGIC   bearer_token='NA'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w=WorkspaceClient()
conn=w.connections.get("youtube_earthquake_conn2")
print(conn)
base_url=f"{conn.options['host']}{conn.options['base_path']}"


# COMMAND ----------

dbutils.widgets.text('catalog_name','youtube_dev','youtube_dev')
catalog_name=dbutils.widgets.get('catalog_name')
print(catalog_name)

# COMMAND ----------

# MAGIC %py
# MAGIC spark.sql(f"use catalog {catalog_name}");
# MAGIC spark.sql("use schema bronze");
# MAGIC spark.sql("create volume if not exists youtube_earthquak_data")
# MAGIC

# COMMAND ----------

import requests
import json
import datetime

url = f"{base_url}summary/all_day.geojson"
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"Error in getting data {url}")
data = response.json()
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
print(catalog_name)
dbutils.fs.put(
    f"/Volumes/{catalog_name}/bronze/youtube_earthquak_data/earthquake_data_{current_date}.json",
    json.dumps(data),
    overwrite=True,
)

# COMMAND ----------


