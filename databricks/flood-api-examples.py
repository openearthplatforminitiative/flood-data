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
from shapely.geometry import Polygon
from math import floor

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
GLOFAS_ROI_CENTRAL_AFRICA = get_config_val("GLOFAS_ROI_CENTRAL_AFRICA")

S3_GLOFAS_PROCESSED_PATH = get_config_val("S3_GLOFAS_PROCESSED_PATH")
GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME = get_config_val("GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME")
GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME = get_config_val("GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME")

GLOFAS_FLOOD_TENDENCIES = get_config_val('GLOFAS_FLOOD_TENDENCIES')
GLOFAS_FLOOD_INTENSITIES = get_config_val('GLOFAS_FLOOD_INTENSITIES')
GLOFAS_FLOOD_PEAK_TIMINGS = get_config_val('GLOFAS_FLOOD_PEAK_TIMINGS')

# COMMAND ----------

lat_min = GLOFAS_ROI_CENTRAL_AFRICA['lat_min']
lat_max = GLOFAS_ROI_CENTRAL_AFRICA['lat_max']
lon_min = GLOFAS_ROI_CENTRAL_AFRICA['lon_min']
lon_max = GLOFAS_ROI_CENTRAL_AFRICA['lon_max']

# COMMAND ----------

date = datetime.utcnow() #- timedelta(days=1)
formatted_date = date.strftime("%Y-%m-%d")
formatted_date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Define functionality**

# COMMAND ----------

def read_with_geopandas(file_path):
    """
    Reads a Parquet file into a GeoDataFrame.
    """

    # Read Parquet into Pandas DataFrame
    df = pd.read_parquet(file_path)

    # Convert WKT strings to geometry objects
    df['geometry'] = df['wkt'].apply(wkt.loads)
    df = gpd.GeoDataFrame(df, geometry='geometry').drop(columns='wkt')

    return df

def get_grid_cell_bounds(lat, lon, grid_size=0.05, precision=3):
    """
    Given a latitude and longitude, find the bounding box of the grid cell
    it falls into.

    The function identifies the grid cell based on the following logic:
    - If the provided point is on the boundary between two cells, the function 
      will assign the point to the cell to its east (right, for longitude) or 
      north (above, for latitude).
    - For example, for a grid_size of 0.05:
      * An input latitude of -5.8 and longitude of 37.75 would result in a 
        bounding box from latitude -5.8 to -5.75 and longitude 37.75 to 37.8.
      * An input latitude of -5.81 and longitude of 37.7501 would result in a 
        bounding box from latitude -5.85 to -5.8 and longitude 37.75 to 37.8.

    Parameters:
    - lat (float): The latitude of the point.
    - lon (float): The longitude of the point.
    - grid_size (float, optional): The size of each grid cell. Defaults to 0.05.
    - precision (int, optional): Number of decimal places to round to. Defaults to 3.

    Returns:
    tuple: Bounding box of the grid cell as (min_latitude, max_latitude, min_longitude, max_longitude).
    """

    # Calculate lower boundaries using math.floor
    min_lon_cell = floor(lon / grid_size) * grid_size
    min_lat_cell = floor(lat / grid_size) * grid_size

    # Use the grid size to find the upper boundaries
    max_lon_cell = min_lon_cell + grid_size
    max_lat_cell = min_lat_cell + grid_size

    # Round the results
    return round(min_lat_cell, precision),\
           round(max_lat_cell, precision),\
           round(min_lon_cell, precision),\
           round(max_lon_cell, precision)

def create_polygon_from_bounds(min_lat, max_lat, 
                               min_lon, max_lon, 
                               buffer=0, precision=3):
    """
    Given the bounds (min_lat, max_lat, min_lon, max_lon), 
    return a Polygon.
    """

    # Define the four corners of the polygon
    bottom_left = (round(min_lon - buffer, precision), 
                   round(min_lat - buffer, precision))
    bottom_right = (round(max_lon + buffer, precision), 
                    round(min_lat - buffer, precision))
    top_right = (round(max_lon + buffer, precision), 
                 round(max_lat + buffer, precision))
    top_left = (round(min_lon - buffer, precision), 
                round(max_lat + buffer, precision))

    # Create and return the polygon
    return Polygon([bottom_left, top_left, top_right, 
                    bottom_right, bottom_left])

# COMMAND ----------

summary_df_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_PROCESSED_PATH, 
                                    formatted_date, GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME)

df_summary = read_with_geopandas(summary_df_file_path)

# COMMAND ----------

detailed_df_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_PROCESSED_PATH, 
                                     formatted_date, GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME)
df_detailed = read_with_geopandas(detailed_df_file_path)

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

print(are_latitudes_equal)  # Should be True
print(are_longitudes_equal)  # Should be True

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Query a point with possibility to fetch neighboring cells**

# COMMAND ----------

# Assume the user provides this in the query
user_lat, user_lon, fetch_neighbors = -5.81, 37.7501, True

# COMMAND ----------

# Get the bounds for the queryed cell
cell_bounds = get_grid_cell_bounds(lat=user_lat, lon=user_lon, 
                                   grid_size=GLOFAS_RESOLUTION, 
                                   precision=GLOFAS_PRECISION)

# Get the deflated geometry to find primary cell
reduced_geometry = create_polygon_from_bounds(*cell_bounds, 
                                              buffer=-GLOFAS_RESOLUTION/2,
                                              precision=GLOFAS_PRECISION)

if fetch_neighbors:
    print('fetching primary cell and neighbors')

    # Get the inflated geometry to find primary cell and neighbors
    expanded_geometry = create_polygon_from_bounds(*cell_bounds, 
                                               buffer=GLOFAS_RESOLUTION/2,
                                               precision=GLOFAS_PRECISION)
    
    # Query the summary dataframe
    all_cells_df = df_summary[df_summary['geometry'].intersects(expanded_geometry)]

    # Define mask for distinguishing between primary cell and neighbors
    primary_cell_mask = all_cells_df['geometry'].intersects(reduced_geometry)

    # Keep primary cell
    primary_cell_df = all_cells_df[primary_cell_mask]

    # Keep only neighboring cells
    neighbors_only_df = all_cells_df[~primary_cell_mask]
else:
    print('Fetching only primary cell')
    # Query the dataframe for the primary cell
    primary_cell_df = df_summary[df_summary['geometry'].intersects(reduced_geometry)]

# COMMAND ----------

all_cells_df

# COMMAND ----------

primary_cell_df

# COMMAND ----------

neighbors_only_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Open and query the threshold data**

# COMMAND ----------

threshold_df_file_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_PROCESSED_THRESH_FILENAME)

df_thresh = read_with_geopandas(threshold_df_file_path)

# COMMAND ----------

primary_cell_df_thresh = df_thresh[df_thresh['geometry'].intersects(reduced_geometry)]

# COMMAND ----------

primary_cell_df_thresh
