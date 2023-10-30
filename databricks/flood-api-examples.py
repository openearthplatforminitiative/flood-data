# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Flood API examples

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os
from datetime import datetime, timedelta
from flood.utils.config import get_config_val
import geopandas as gpd
import pandas as pd
from shapely import wkt 
from shapely.geometry import Point

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch configuration parameters**

# COMMAND ----------

PYTHON_PREFIX = get_config_val("PYTHON_PREFIX")

S3_GLOFAS_AUX_DATA_PATH = get_config_val("S3_GLOFAS_AUX_DATA_PATH")
GLOFAS_RET_PRD_THRESH_VALS = get_config_val("GLOFAS_RET_PRD_THRESH_VALS")
GLOFAS_PRECISION = get_config_val("GLOFAS_PRECISION")
GLOFAS_RESOLUTION = get_config_val('GLOFAS_RESOLUTION')
GLOFAS_PROCESSED_THRESH_FILENAME = get_config_val("GLOFAS_PROCESSED_THRESH_FILENAME")

S3_GLOFAS_PROCESSED_PATH = get_config_val("S3_GLOFAS_PROCESSED_PATH")
GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME = get_config_val("GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME")
GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME = get_config_val("GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME")

GLOFAS_FLOOD_TENDENCIES = get_config_val('GLOFAS_FLOOD_TENDENCIES')
GLOFAS_FLOOD_INTENSITIES = get_config_val('GLOFAS_FLOOD_INTENSITIES')
GLOFAS_FLOOD_PEAK_TIMINGS = get_config_val('GLOFAS_FLOOD_PEAK_TIMINGS')

# COMMAND ----------

date = datetime.utcnow() #- timedelta(days=3)
formatted_date = date.strftime("%Y-%m-%d")
formatted_date

# COMMAND ----------

def read_with_geopandas(file_path):

    # Read Parquet into Pandas DataFrame
    df = pd.read_parquet(file_path)

    # Convert WKT strings to geometry objects
    df['geometry'] = df['wkt'].apply(wkt.loads)
    df = gpd.GeoDataFrame(df, geometry='geometry')

    # Optionally, you can drop the 'wkt' column now
    df.drop(columns='wkt', inplace=True)

    return df

# COMMAND ----------

summary_df_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_PROCESSED_PATH, 
                                    formatted_date, GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME)

df_summary = read_with_geopandas(summary_df_file_path)

# COMMAND ----------

detailed_df_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_PROCESSED_PATH, 
                                     formatted_date, GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME)

df_detailed = read_with_geopandas(detailed_df_file_path)

# COMMAND ----------

df_summary.info()

# COMMAND ----------

df_summary.head(5)

# COMMAND ----------

df_detailed.info()

# COMMAND ----------

df_detailed.head(5)

# COMMAND ----------

# Check that the set of latitude and longitude 
# values from both dataframes match

# Convert columns to sets
lat_set_df1 = set(df_summary['latitude'])
lat_set_df2 = set(df_detailed['latitude'])

lon_set_df1 = set(df_summary['longitude'])
lon_set_df2 = set(df_detailed['longitude'])

# Check if sets are equal for both latitude and longitude
are_latitudes_equal = lat_set_df1 == lat_set_df2
are_longitudes_equal = lon_set_df1 == lon_set_df2

print(are_latitudes_equal)  # True
print(are_longitudes_equal)  # True

# COMMAND ----------

# Define a very small offset for edge cases
# when a point is on a cell border or corner
EPSILON = 1e-9
PRECISION = 8 # one less than the epsilon

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Query a point**

# COMMAND ----------

# Given lat/lon coordinates
user_lat, user_lon = 16.50, 28.73

# Round to 8 decimal places and adjust with epsilon. 
# This ensures that the queried lat, lon will never fall on the
# border between two cells (or at the corner between
# four cells). As a result, if the user queries latitude = 17.0 and
# longitude = 16.55, the considered cell will be to the bottom right,
# i.e., the one with center (16.975, 16.575).
lat, lon = round(user_lat, PRECISION), round(user_lon, PRECISION)

# Adjust the point slightly
point = Point(lon + EPSILON, lat - EPSILON)

# Use the contains method to filter the dataframe
filtered_summary_df = df_summary[df_summary['geometry'].contains(point)]

# COMMAND ----------

filtered_summary_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch neighboring cells**

# COMMAND ----------

# Assuming the primary cell has already been found and it exists
primary_cell_geometry = filtered_summary_df.iloc[0].geometry

# Get the bounding box (envelope) of the primary cell and expand it
# Assuming GLOFAS_RESOLUTION / 2 is the half-length of a cell side
expanded_bbox = primary_cell_geometry.envelope.buffer(1.5 * GLOFAS_RESOLUTION / 2)

# Query using the expanded bounding box to get neighbors
neighbors_df = df_summary[df_summary['geometry'].intersects(expanded_bbox)]

# To exclude the primary cell from the result
neighbors_df = neighbors_df[neighbors_df.geometry != primary_cell_geometry]

# COMMAND ----------

neighbors_df
