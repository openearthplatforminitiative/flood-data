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
from flood.api.client import GloFASClient
from flood.api.config import GloFASAPIConfig

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Hardcoded configuration parameters (should be placed in config file in the future)**

# COMMAND ----------

S3_OPEN_EPI_PATH = os.path.join('mnt','openepi-storage')
S3_GLOFAS_PATH = os.path.join(S3_OPEN_EPI_PATH, 'glofas')
S3_GLOFAS_API_PATH = os.path.join(S3_GLOFAS_PATH, 'api-downloads')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Configure client**

# COMMAND ----------

# Define API access variables
url = 'https://cds.climate.copernicus.eu/api/v2'
user_id = dbutils.secrets.get(scope="openepi", key="cds_user_id")
api_key = dbutils.secrets.get(scope="openepi", key="cds_api_key")

# Create client 
client = GloFASClient(url, f'{user_id}:{api_key}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Configure request parameters**

# COMMAND ----------

# Specify date for query
from datetime import datetime, timedelta

# Whether or not we need to take yesterday's day or not needs to be confirmed
# by monitoring the CDS website. According to their documentation, the current date
# is the latest queryable date from the API, so taking -1 probably isn't necessary.
# date_for_request -= timedelta(days=1)
date_for_request = datetime.utcnow()

# Specify the desired forecast horizon. This variable can be a list of multiples of 
# 24 all the way up to 720, i.e. leadtime_hour = ['24', '48', ..., '696', '720'].
# The simplest approach (for now) is to probably fetch one 24-hour period at a time.
# This is crucial when the ROI becomes large because of the file size.
leadtime_hour = '24'

# Specify the ROI. The data's resolution is 0.05°x0.05°. To ensure the entirety of
# the desired ROI is retrieved, we fetch a slightly larger ROI and trim it down to 
# the true ROI later during the actual processing of the GRIB file.

lat_min = -6.0 # South
lat_max = 17.0 # North
lon_min = -18.0 # East
lon_max = 52.0 # West
area = [lat_max+0.1, lon_min-0.1, lat_min-0.1, lon_max+0.1]

# Define the config
config = GloFASAPIConfig(
    year=date_for_request.year,
    month=date_for_request.month,
    day=date_for_request.day,
    leadtime_hour=leadtime_hour,
    area=area
)

# Convert config to a dictionary
request_params = config.to_dict()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create target folder**

# COMMAND ----------

target_folder = os.path.join(S3_GLOFAS_API_PATH, formatted_date)
dbutils.fs.mkdirs(os.path.join('dbfs:', target_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch the data**

# COMMAND ----------

# Define target filepath
formatted_date = date_for_request.strftime("%Y-%m-%d")
target_filename = f'download-{leadtime_hour}.grib'
target_file_path = os.path.join('/dbfs', target_folder, target_filename)

# Fetch the data
client.fetch_grib_data(request_params, target_file_path)
