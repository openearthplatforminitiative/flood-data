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
from tqdm import tqdm
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

USE_CONTROL_MEMBER_IN_ENSEMBLE = get_config_val("USE_CONTROL_MEMBER_IN_ENSEMBLE")

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

date_for_request = datetime.utcnow() #- timedelta(days=1)
formatted_date = date_for_request.strftime("%Y-%m-%d")

# leadtime_hour can be one of '24', '48', ..., '696', '720'.
# leadtime_hour = '24'
leadtime_hours = [
            '24', '48', '72',
            '96', '120', '144',
            '168', '192', '216',
            '240', '264', '288',
            '312', '336', '360',
            '384', '408', '432',
            '456', '480', '504',
            '528', '552', '576',
            '600', '624', '648',
            '672', '696', '720',
        ]

converter = RasterConverter()

#Create target folder
filtered_parquet_folder = os.path.join(S3_GLOFAS_FILTERED_PATH, formatted_date)
dbutils.fs.mkdirs(os.path.join(DBUTILS_PREFIX, filtered_parquet_folder))

# Open upstream area NetCDF file and restrict to area of interest
upstream_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_UPSTREAM_FILENAME)
ds_upstream = open_dataset(upstream_file_path)

if USE_CONTROL_MEMBER_IN_ENSEMBLE:
    print('Combining control and ensemble')
else:
    print('Using only ensemble')

for l_hour in tqdm(leadtime_hours):
    discharge_filename = f'download-{l_hour}.grib'
    discharge_file_path = os.path.join(PYTHON_PREFIX, 
                                       S3_GLOFAS_DOWNLOADS_PATH, 
                                       formatted_date, 
                                       discharge_filename)

    # Load control forecast (cf)
    # Could be emty if it wasn't downloaded i API query
    ds_cf = open_dataset(discharge_file_path,
                         backend_kwargs={'filter_by_keys': {'dataType': 'cf'}})

    # Load perturbed forecast (pf)
    ds_pf = open_dataset(discharge_file_path,
                         backend_kwargs={'filter_by_keys': {'dataType': 'pf'}})
    
    if USE_CONTROL_MEMBER_IN_ENSEMBLE:
        ds_discharge = xr.concat([ds_cf, ds_pf], dim='number')
    else:
        ds_discharge = ds_pf

    # ds_discharge = open_dataset(discharge_file_path)

    # Restrict discharge data to area of interest
    ds_discharge = restrict_dataset_area(ds_discharge,
                                         lat_min, lat_max,
                                         lon_min, lon_max,
                                         buffer) 

    # Apply upstream filtering
    filtered_ds = apply_upstream_threshold(ds_discharge, 
                                           ds_upstream, 
                                           threshold_area=GLOFAS_UPSTREAM_THRESHOLD,
                                           buffer=buffer)
    
    # Convert to pandas dataframe
    filtered_df = converter.dataset_to_dataframe(filtered_ds['dis24'], 
                                                 cols_to_drop=['surface'], 
                                                 drop_na_subset=['dis24'], 
                                                 drop_index=False)
    
    # Save to Parquet
    filtered_parquet_filename = f'filtered-{l_hour}.parquet'
    filtered_parquet_file_path = os.path.join(PYTHON_PREFIX, filtered_parquet_folder, filtered_parquet_filename)
    converter.dataframe_to_parquet(filtered_df, filtered_parquet_file_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls /dbfs/mnt/openepi-storage/glofas/api-downloads/2023-10-26
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/openepi-storage/glofas/filtered/2023-10-26
