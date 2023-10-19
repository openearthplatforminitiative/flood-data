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
from flood.etl.utils import load_dataset
from flood.etl.filter_by_upstream import apply_upstream_threshold
from flood.etl.raster_converter import RasterConverter

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Hardcoded configuration parameters (should be placed in config file in the future)**

# COMMAND ----------

ROI_CENTRAL_AFRICA = {'lat_min': -6.0, 'lat_max': 17.0,
                      'lon_min': -18.0, 'lon_max': 52.0}
RESOLUTION = 0.05
BUFFER = RESOLUTION / 4
UPSTREAM_THRESHOLD = 250000 # m^2

S3_OPEN_EPI_PATH = os.path.join('mnt','openepi-storage')
S3_GLOFAS_PATH = os.path.join(S3_OPEN_EPI_PATH, 'glofas')
S3_GLOFAS_API_PATH = os.path.join(S3_GLOFAS_PATH, 'api-downloads')
S3_AUX_DATA_PATH = os.path.join(S3_GLOFAS_PATH, 'auxiliary-data')
S3_FILTERED_PATH = os.path.join(S3_GLOFAS_PATH, 'filtered')
UPSTREAM_FILENAME = 'uparea_glofas_v4_0.nc'

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

discharge_file_path = os.path.join('/dbfs', S3_GLOFAS_API_PATH, formatted_date, discharge_filename)
ds_discharge = load_dataset(discharge_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Restrict discharge data to area of interest**

# COMMAND ----------

lat_min, lat_max = ROI_CENTRAL_AFRICA['lat_min'], ROI_CENTRAL_AFRICA['lat_max']
lon_min, lon_max = ROI_CENTRAL_AFRICA['lon_min'], ROI_CENTRAL_AFRICA['lon_max']

# Perform xarray filtering
ds_discharge = ds_discharge.sel(
    latitude=slice(lat_max + BUFFER, lat_min - BUFFER),
    longitude=slice(lon_min  - BUFFER, lon_max  + BUFFER))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Open upstream area NetCDF file and restrict to area of interest**

# COMMAND ----------

upstream_file_path = os.path.join('/dbfs', S3_AUX_DATA_PATH, UPSTREAM_FILENAME)
ds_upstream = load_dataset(upstream_file_path)

# Perform xarray filtering
ds_upstream = ds_upstream.sel(
    latitude=slice(lat_max + BUFFER, lat_min - BUFFER),
    longitude=slice(lon_min  - BUFFER, lon_max  + BUFFER))

# COMMAND ----------

# MAGIC %md 
# MAGIC **Apply upstream filtering**

# COMMAND ----------

filtered_ds = apply_upstream_threshold(ds_discharge, 
                                       ds_upstream, 
                                       threshold_area=UPSTREAM_THRESHOLD)

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

filtered_parquet_folder = os.path.join(S3_FILTERED_PATH, formatted_date)
dbutils.fs.mkdirs(os.path.join('dbfs:', filtered_parquet_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Save to Parquet**

# COMMAND ----------

filtered_parquet_filename = f'filtered-{leadtime_hour}.parquet'
filtered_parquet_file_path = os.path.join('/dbfs', filtered_parquet_folder, filtered_parquet_filename)

converter.dataframe_to_parquet(filtered_df, filtered_parquet_file_path)
