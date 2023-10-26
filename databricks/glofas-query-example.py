# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Querying GloFAS API and saving resulting GRIB files to mounted S3 bucket

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os
from datetime import datetime, timedelta
from flood.api.client import GloFASClient
from flood.api.config import GloFASAPIConfig
from flood.utils.config import get_config_val

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

USE_CONTROL_MEMBER_IN_ENSEMBLE = get_config_val("USE_CONTROL_MEMBER_IN_ENSEMBLE")

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
# Whether or not we need to take yesterday's day or not needs to be confirmed
# by monitoring the CDS website. According to their documentation, the current date
# is the latest queryable date from the API, so taking -1 probably isn't necessary.
# date_for_request -= timedelta(days=1)
date_for_request = datetime.utcnow() #- timedelta(days=1)
formatted_date = date_for_request.strftime("%Y-%m-%d")

# Specify the desired forecast horizon. This variable can be a list of multiples of 
# 24 all the way up to 720, i.e. leadtime_hour = ['24', '48', ..., '696', '720'].
# The simplest approach (for now) is to probably fetch one 24-hour period at a time.
# This is crucial when the ROI becomes large because of the file size.
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

# Specify the ROI. The data's resolution is 0.05°x0.05°. To ensure the entirety of
# the desired ROI is retrieved, we fetch a slightly larger ROI and trim it down to 
# the true ROI later during the actual processing of the GRIB file.

area = [lat_max+query_buffer, 
        lon_min-query_buffer, 
        lat_min-query_buffer, 
        lon_max+query_buffer]

if USE_CONTROL_MEMBER_IN_ENSEMBLE:
        product_type = ['control_forecast', 'ensemble_perturbed_forecasts']
        print('Retrieving both control and ensemble')
else: 
        product_type = 'ensemble_perturbed_forecasts'
        print('Retrieving only ensemble')

# Define the config
# config = GloFASAPIConfig(
#     year=date_for_request.year,
#     month=date_for_request.month,
#     day=date_for_request.day,
#     leadtime_hour=leadtime_hour,
#     area=area
# )

# Convert config to a dictionary
# request_params = config.to_dict()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create target folder**

# COMMAND ----------

target_folder = os.path.join(S3_GLOFAS_DOWNLOADS_PATH, formatted_date)
dbutils.fs.mkdirs(os.path.join(DBUTILS_PREFIX, target_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch the data**

# COMMAND ----------

for l_hour in leadtime_hours:
    # Define target filepath
    target_filename = f'download-{l_hour}.grib'
    target_file_path = os.path.join(PYTHON_PREFIX, target_folder, target_filename)

        # Define the config
    config = GloFASAPIConfig(
        year=date_for_request.year,
        month=date_for_request.month,
        day=date_for_request.day,
        leadtime_hour=l_hour,
        area=area,
        product_type=product_type
    )

    # Convert config to a dictionary
    request_params = config.to_dict()

    # Fetch the data
    client.fetch_grib_data(request_params, target_file_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/openepi-storage/glofas/api-downloads/2023-10-26

# COMMAND ----------


