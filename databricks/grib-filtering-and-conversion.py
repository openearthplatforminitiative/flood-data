# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Discharge (GRIB data) filtering by upstream area and saving as Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import xarray as xr
import os
from datetime import datetime, timedelta
from flood.etl.utils import open_dataset, restrict_dataset_area
from flood.etl.filter_by_upstream import apply_upstream_threshold
from flood.etl.raster_converter import RasterConverter
from flood.utils.config import get_config_val

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch configuration parameters**

# COMMAND ----------

PYTHON_PREFIX = get_config_val("PYTHON_PREFIX")
DBUTILS_PREFIX = get_config_val("DBUTILS_PREFIX")

S3_GLOFAS_DOWNLOADS_PATH = get_config_val("S3_GLOFAS_DOWNLOADS_PATH")
S3_GLOFAS_AUX_DATA_PATH = get_config_val("S3_GLOFAS_AUX_DATA_PATH")
S3_GLOFAS_FILTERED_PATH = get_config_val("S3_GLOFAS_FILTERED_PATH")
GLOFAS_UPSTREAM_FILENAME = get_config_val("GLOFAS_UPSTREAM_FILENAME")

GLOFAS_ROI_CENTRAL_AFRICA = get_config_val("GLOFAS_ROI_CENTRAL_AFRICA")
GLOFAS_RESOLUTION = get_config_val("GLOFAS_RESOLUTION")
GLOFAS_BUFFER_DIV = get_config_val("GLOFAS_BUFFER_DIV")
GLOFAS_UPSTREAM_THRESHOLD = get_config_val("GLOFAS_UPSTREAM_THRESHOLD")

# COMMAND ----------

buffer = GLOFAS_RESOLUTION / GLOFAS_BUFFER_DIV
lat_min = GLOFAS_ROI_CENTRAL_AFRICA['lat_min']
lat_max = GLOFAS_ROI_CENTRAL_AFRICA['lat_max']
lon_min = GLOFAS_ROI_CENTRAL_AFRICA['lon_min']
lon_max = GLOFAS_ROI_CENTRAL_AFRICA['lon_max']

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Define path to read discharge from and load GRIB dataset**

# COMMAND ----------

date_for_request = datetime.utcnow()
formatted_date = date_for_request.strftime("%Y-%m-%d")

# leadtime_hour can be one of '24', '48', ..., '696', '720'.
leadtime_hour = '24'
discharge_filename = f'download-{leadtime_hour}.grib'

discharge_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_DOWNLOADS_PATH, formatted_date, discharge_filename)
ds_discharge = open_dataset(discharge_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Restrict discharge data to area of interest**

# COMMAND ----------

ds_discharge = restrict_dataset_area(ds_discharge,
                                     lat_min, lat_max,
                                     lon_min, lon_max,
                                     buffer) 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Open upstream area NetCDF file and restrict to area of interest**

# COMMAND ----------

upstream_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_UPSTREAM_FILENAME)
ds_upstream = open_dataset(upstream_file_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Apply upstream filtering**

# COMMAND ----------

filtered_ds = apply_upstream_threshold(ds_discharge, 
                                       ds_upstream, 
                                       threshold_area=GLOFAS_UPSTREAM_THRESHOLD,
                                       buffer=buffer)

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert to pandas dataframe**

# COMMAND ----------

converter = RasterConverter()
filtered_df = converter.dataset_to_dataframe(filtered_ds['dis24'], 
                                             cols_to_drop=['surface'], 
                                             drop_na_subset=['dis24'], 
                                             drop_index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create target folder**

# COMMAND ----------

filtered_parquet_folder = os.path.join(S3_GLOFAS_FILTERED_PATH, formatted_date)
dbutils.fs.mkdirs(os.path.join(DBUTILS_PREFIX, filtered_parquet_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Save to Parquet**

# COMMAND ----------

filtered_parquet_filename = f'filtered-{leadtime_hour}.parquet'
filtered_parquet_file_path = os.path.join(PYTHON_PREFIX, filtered_parquet_folder, filtered_parquet_filename)
converter.dataframe_to_parquet(filtered_df, filtered_parquet_file_path)
