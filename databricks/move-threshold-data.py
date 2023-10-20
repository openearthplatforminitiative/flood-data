# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Guide to create folder and move threshold data to mounted cloud storage
# MAGIC
# MAGIC **Only needs to be performed once (done)**
# MAGIC
# MAGIC This guide assumes the files `RP2ythresholds_GloFASv40.nc`, `RP5ythresholds_GloFASv40.nc`, and `RP20ythresholds_GloFASv40.nc` have been uploaded to DBFS in `FileStore/flood`.
# MAGIC

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
# MAGIC **Fetch configuration parameters**

# COMMAND ----------

DBUTILS_PREFIX = get_config_val("DBUTILS_PREFIX")
S3_GLOFAS_AUX_DATA_PATH = get_config_val("S3_GLOFAS_AUX_DATA_PATH")
GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES = get_config_val("GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES")
GLOFAS_RET_PRD_THRESH_VALS = get_config_val("GLOFAS_RET_PRD_THRESH_VALS")

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

target_threshold_folder = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH)

for thresold in GLOFAS_RET_PRD_THRESH_VALS:
    filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(thresold)]
    current_threshold_data_path = os.path.join(DBUTILS_PREFIX,'FileStore', 'flood', filename)
    dbutils.fs.mv(current_threshold_data_path, target_threshold_folder)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Check that the files are present in target folder**

# COMMAND ----------

dbutils.fs.ls(target_threshold_folder)
