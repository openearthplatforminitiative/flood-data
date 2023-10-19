# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Guide to create folder and move upstream data to mounted cloud storage
# MAGIC
# MAGIC **Only needs to be performed once (done)**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Hardcoded configuration parameters (should be placed in config file in the future)**

# COMMAND ----------

S3_OPEN_EPI_PATH = os.path.join('mnt', 'openepi-storage')
S3_GLOFAS_PATH = os.path.join(S3_OPEN_EPI_PATH, 'glofas')
S3_AUX_DATA_PATH = os.path.join(S3_GLOFAS_PATH, 'auxiliary-data')
UPSTREAM_FILENAME = 'uparea_glofas_v4_0.nc'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create target folder**

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join('dbfs:', S3_AUX_DATA_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Move data**

# COMMAND ----------

current_upstream_data_path = os.path.join('dbfs:','FileStore', 'flood', UPSTREAM_FILENAME)
target_upstream_folder = os.path.join('dbfs:', S3_AUX_DATA_PATH)
dbutils.fs.mv(current_upstream_data_path, target_upstream_folder)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Check that the file is present in target folder**

# COMMAND ----------

dbutils.fs.ls(target_upstream_folder)
