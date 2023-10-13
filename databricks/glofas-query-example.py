# Databricks notebook source
# Import necessary functionality
from flood.api.client import GloFASClient
from flood.api.config import GloFASAPIConfig

# Define API access variables
url = 'https://cds.climate.copernicus.eu/api/v2'
user_id = dbutils.secrets.get(scope="openepi", key="cds_user_id")
api_key = dbutils.secrets.get(scope="openepi", key="cds_api_key")

# Create client 
client = GloFASClient(url, f'{user_id}:{api_key}')

# COMMAND ----------

# Specify date for query
from datetime import datetime, timedelta
date_for_request = datetime.utcnow()

# Whether or not we need to take yesterday's day or not needs to be confirmed
# by monitoring the CDS website. According to their documentation, the current date
# is the latest queryable date from the API, so taking -1 probably isn't necessary.
# date_for_request -= timedelta(days=1)

year = date_for_request.strftime("%Y")
month = date_for_request.strftime("%m")
day = date_for_request.strftime("%d")

# Specify the desired forecast horizon. This variable can be a list of multiples of 
# 24 all the way up to 720, i.e. leadtime_hour = ['24', '48', ..., '696', '720'].
# The simplest approach (for now) is to probably fetch one 24-hour period at a time.
# This is crucial when the ROI becomes large because of the file size
leadtime_hour = '24'

# Specify the ROI. The way the API interprets these values is a little strange. The data's resolution
# is 0.05°x0.05°. 
# If the desired ROI should start at 17° latitude and finish at -6° longitude,
# the CDS API needs to get 17-resolution/2 and -6+3*resolution/2 as parameters. If the desired ROI 
# should start at -18° longitude and finish at 52° longitude, the CDS API needs to get -18+3*resolution/2 
# and 52-resolution/2 as parameters. 
# This makes little sense so a more robust method is to maybe filter the retrieved GRIB file 
# to the specific ROI after download. For example:
# ds_new = ds.sel(latitude=slice(lat_max, lat_min),longitude=slice(lon_min, lon_max))

lat_min = -6 # South
lat_max = 17 # North
lon_min = -18 # East
lon_max = 52 # West
area = [lat_max, lon_min, lat_min, lon_max]

# Define the config
config = GloFASAPIConfig(
    year=year,
    month=month,
    day=day,
    leadtime_hour=leadtime_hour,
    area=area
)

# Convert config to a dictionary
request_params = config.to_dict()

# COMMAND ----------

# Define target filepath
target_filepath = f'download_{year}_{month}_{day}_{leadtime_hour}.grib'

# Fetch the data
client.fetch_grib_data(request_params, target_filepath)

# COMMAND ----------


