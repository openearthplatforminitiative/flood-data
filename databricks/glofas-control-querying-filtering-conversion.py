# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Querying GloFAS API for control forecast, filtering by upstream area, and saving resulting parquet file to mounted S3 bucket

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os
from datetime import datetime, timedelta
from flood.api.client import GloFASClient
from flood.api.config import GloFASAPIConfig
from flood.etl.raster_converter import RasterConverter
from flood.utils.config import get_config_val
from flood.etl.utils import open_dataset, restrict_dataset_area
from flood.etl.filter_by_upstream import apply_upstream_threshold

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch configuration parameters**

# COMMAND ----------

PYTHON_PREFIX = get_config_val("PYTHON_PREFIX")
DBUTILS_PREFIX = get_config_val("DBUTILS_PREFIX")

S3_GLOFAS_DOWNLOADS_PATH = get_config_val("S3_GLOFAS_DOWNLOADS_PATH")
GLOFAS_API_URL = get_config_val("GLOFAS_API_URL")
GLOFAS_ROI_CENTRAL_AFRICA = get_config_val("GLOFAS_ROI_CENTRAL_AFRICA")
GLOFAS_RESOLUTION = get_config_val("GLOFAS_RESOLUTION")
GLOFAS_BUFFER_MULT = get_config_val("GLOFAS_BUFFER_MULT")
GLOFAS_UPSTREAM_FILENAME = get_config_val("GLOFAS_UPSTREAM_FILENAME")
S3_GLOFAS_FILTERED_PATH = get_config_val("S3_GLOFAS_FILTERED_PATH")
S3_GLOFAS_AUX_DATA_PATH = get_config_val("S3_GLOFAS_AUX_DATA_PATH")
GLOFAS_BUFFER_DIV = get_config_val("GLOFAS_BUFFER_DIV")
GLOFAS_UPSTREAM_THRESHOLD = get_config_val("GLOFAS_UPSTREAM_THRESHOLD")

# COMMAND ----------

query_buffer = GLOFAS_RESOLUTION * GLOFAS_BUFFER_MULT
lat_min = GLOFAS_ROI_CENTRAL_AFRICA['lat_min']
lat_max = GLOFAS_ROI_CENTRAL_AFRICA['lat_max']
lon_min = GLOFAS_ROI_CENTRAL_AFRICA['lon_min']
lon_max = GLOFAS_ROI_CENTRAL_AFRICA['lon_max']

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Configure client**

# COMMAND ----------

# Define API access variables
user_id = dbutils.secrets.get(scope="openepi", key="cds_user_id")
api_key = dbutils.secrets.get(scope="openepi", key="cds_api_key")

# Create client 
client = GloFASClient(GLOFAS_API_URL, f'{user_id}:{api_key}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Configure request parameters**

# COMMAND ----------

# Specify date for query
# We need to take yesterday's forecast for the control forecast.
date_for_folder = datetime.utcnow() #- timedelta(days=1)
date_for_request = date_for_folder - timedelta(days=1)

formatted_date = date_for_folder.strftime("%Y-%m-%d")

# Specify the desired forecast horizon to be 24 hours.
leadtime_hour = '24'

# Specify the ROI. The data's resolution is 0.05°x0.05°. To ensure the entirety of
# the desired ROI is retrieved, we fetch a slightly larger ROI and trim it down to 
# the true ROI later during the actual processing of the GRIB file.

area = [lat_max+query_buffer, 
        lon_min-query_buffer, 
        lat_min-query_buffer, 
        lon_max+query_buffer]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create target folder**

# COMMAND ----------

target_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_DOWNLOADS_PATH, formatted_date)
print(target_folder)
os.makedirs(target_folder, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch the data**

# COMMAND ----------

# Define target filepath
control_filename = 'control.grib'
control_file_path = os.path.join(target_folder, control_filename)

    # Define the config
config = GloFASAPIConfig(
    year=date_for_request.year,
    month=date_for_request.month,
    day=date_for_request.day,
    leadtime_hour=leadtime_hour,
    area=area,
    product_type='control_forecast'
)

# Convert config to a dictionary
request_params = config.to_dict()

# COMMAND ----------

# Fetch the data
client.fetch_grib_data(request_params, control_file_path)

# COMMAND ----------

converter = RasterConverter()

buffer = GLOFAS_RESOLUTION / GLOFAS_BUFFER_DIV

#Create target folder
filtered_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_FILTERED_PATH, formatted_date)
os.makedirs(filtered_parquet_folder, exist_ok=True)

# Open upstream area NetCDF file and restrict to area of interest
upstream_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_UPSTREAM_FILENAME)
ds_upstream = open_dataset(upstream_file_path)

ds_control = open_dataset(control_file_path)

# COMMAND ----------

# Restrict discharge data to area of interest
ds_control = restrict_dataset_area(ds_control,
                                   lat_min, lat_max,
                                   lon_min, lon_max,
                                   buffer) 

# Apply upstream filtering
filtered_control_ds = apply_upstream_threshold(ds_control, 
                                       ds_upstream, 
                                       threshold_area=GLOFAS_UPSTREAM_THRESHOLD,
                                       buffer=buffer)

# Convert to pandas dataframe
filtered_control_df = converter.dataset_to_dataframe(filtered_control_ds['dis24'], 
                                             cols_to_drop=['surface'],
                                             drop_na_subset=['dis24'],
                                             drop_index=False)

# COMMAND ----------

filtered_control_df.info()

# COMMAND ----------

# Save to Parquet
filtered_control_parquet_filename = f'control.parquet'
filtered_control_parquet_file_path = os.path.join(filtered_parquet_folder, filtered_control_parquet_filename)
converter.dataframe_to_parquet(filtered_control_df, filtered_control_parquet_file_path)

# COMMAND ----------

os.listdir(filtered_parquet_folder)
