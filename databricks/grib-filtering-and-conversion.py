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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Define path to read discharge from and load GRIB dataset**

# COMMAND ----------

date_for_request = datetime.utcnow()

year = date_for_request.strftime("%Y")
month = date_for_request.strftime("%m")
day = date_for_request.strftime("%d")

# leadtime_hour can be one of '24', '48', ..., '696', '720'.
leadtime_hour = '24'

discharge_file_path = f'download_{year}_{month}_{day}_{leadtime_hour}.grib'
ds_discharge= load_dataset(discharge_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Restrict discharge data to area of interest**

# COMMAND ----------

lat_min, lat_max = ROI_CENTRAL_AFRICA['lat_min'], ROI_CENTRAL_AFRICA['lat_max']
lon_min, lon_max = ROI_CENTRAL_AFRICA['lon_min'], ROI_CENTRAL_AFRICA['lon_max']
buffer = BUFFER

ds_discharge = ds_discharge.sel(
    latitude=slice(lat_max + buffer, lat_min - buffer),
    longitude=slice(lon_min  - buffer, lon_max  + buffer))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Open upstream area NetCDF file and restrict to area of interest**

# COMMAND ----------

upstream_file_path = '/dbfs/FileStore/flood/uparea_glofas_v4_0.nc'
ds_upstream = load_dataset(upstream_file_path)
ds_upstream = ds_upstream.sel(
    latitude=slice(lat_max + buffer, lat_min - buffer),
    longitude=slice(lon_min  - buffer, lon_max  + buffer))

# COMMAND ----------

# MAGIC %md 
# MAGIC **Apply upstream filtering**

# COMMAND ----------

upstream_threshold = UPSTREAM_THRESHOLD
filtered_ds = apply_upstream_threshold(ds_discharge, ds_upstream, threshold_area=upstream_threshold)

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
# MAGIC **Save to Parquet**

# COMMAND ----------

filtered_parquet_filename = f'filtered_{year}_{month}_{day}_{leadtime_hour}.parquet'
# filtered_parquet_folder = f'data/{year}-{month}-{day}/' # Should create a folder first
filtered_parquet_folder = ''
filtered_parquet_file_path = filtered_parquet_folder + filtered_parquet_filename
converter.dataframe_to_parquet(filtered_df, filtered_parquet_file_path)
