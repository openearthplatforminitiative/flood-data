# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Threshold data conversion from raster (NetCDF) to Parquet
# MAGIC
# MAGIC **Only needs to be performed once (done)**
# MAGIC
# MAGIC This guide assumes the files `RP2ythresholds_GloFASv40.nc`, `RP5ythresholds_GloFASv40.nc`, and `RP20ythresholds_GloFASv40.nc` have been moved to the auxiliary data folder of the mounted S3 bucket.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os
from flood.utils.config import get_config_val
from flood.etl.raster_converter import RasterConverter

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch configuration parameters**

# COMMAND ----------

PYTHON_PREFIX = get_config_val("PYTHON_PREFIX")
DBUTILS_PREFIX = get_config_val("DBUTILS_PREFIX")

S3_GLOFAS_AUX_DATA_PATH = get_config_val("S3_GLOFAS_AUX_DATA_PATH")
GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES = get_config_val("GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES")
GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES = get_config_val("GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES")
GLOFAS_RET_PRD_THRESH_VALS = get_config_val("GLOFAS_RET_PRD_THRESH_VALS")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Convert and write data**

# COMMAND ----------

converter = RasterConverter()
target_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH)

for threshold in GLOFAS_RET_PRD_THRESH_VALS:
    raster_filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(threshold)]
    parquet_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]

    raster_filepath = os.path.join(target_parquet_folder, raster_filename)
    parquet_filepath = os.path.join(target_parquet_folder, parquet_filename)

    converter.file_to_parquet(raster_filepath, parquet_filepath, 
                              cols_to_drop=['wgs_1984'], 
                              cols_to_rename={'lat': 'latitude', 'lon': 'longitude'},
                              drop_index=False, save_index=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Check that the files are present in target folder**

# COMMAND ----------

os.listdir(target_parquet_folder)
