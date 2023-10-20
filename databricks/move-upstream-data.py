# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Guide to create folder and move upstream data to mounted cloud storage
# MAGIC
# MAGIC **Only needs to be performed once (done)**
# MAGIC
# MAGIC This guide assumes the file `uparea_glofas_v4_0.nc` has been uploaded to DBFS in `FileStore/flood`.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os
from flood.utils.config import get_config_val

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Hardcoded configuration parameters (should be placed in config file in the future)**

# COMMAND ----------

PYTHON_PREFIX = get_config_val("PYTHON_PREFIX")
DBUTILS_PREFIX = get_config_val("DBUTILS_PREFIX")
S3_GLOFAS_AUX_DATA_PATH = get_config_val("S3_GLOFAS_AUX_DATA_PATH")
GLOFAS_UPSTREAM_FILENAME = get_config_val("GLOFAS_UPSTREAM_FILENAME")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create target folder**

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Move data**

# COMMAND ----------

current_upstream_data_path = os.path.join(DBUTILS_PREFIX,'FileStore', 'flood', GLOFAS_UPSTREAM_FILENAME)
target_upstream_folder = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH)
dbutils.fs.mv(current_upstream_data_path, target_upstream_folder)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Check that the file is present in target folder**

# COMMAND ----------

dbutils.fs.ls(target_upstream_folder)
