# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Flood metrics computation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/openepi-storage/glofas/processed

# COMMAND ----------

import os
from datetime import datetime, timedelta
from flood.utils.config import get_config_val
from flood.spark.transforms import (create_round_udf, 
                                    compute_flood_tendency,
                                    compute_flood_intensity,
                                    compute_flood_peak_timing,
                                    compute_flood_threshold_percentages,
                                    add_geometry)
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType,
                               StructField, 
                               FloatType, 
                               LongType, 
                               DoubleType)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Boilerplate code if the notebook is run outside Databricks**

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession

# Check if a Spark session exists, if not, create one
if 'spark' not in locals():
    spark = SparkSession.builder \
        .appName("FloodMetricsComputation") \
        .getOrCreate()

# If you need the Spark context, you can get it from the Spark session
if 'sc' not in locals():
    sc = spark.sparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch configuration parameters**

# COMMAND ----------

PYTHON_PREFIX = get_config_val("PYTHON_PREFIX")
DBUTILS_PREFIX = get_config_val("DBUTILS_PREFIX")

S3_GLOFAS_AUX_DATA_PATH = get_config_val("S3_GLOFAS_AUX_DATA_PATH")
GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES = get_config_val("GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES")
GLOFAS_RET_PRD_THRESH_VALS = get_config_val("GLOFAS_RET_PRD_THRESH_VALS")
GLOFAS_PRECISION = get_config_val("GLOFAS_PRECISION")
GLOFAS_RESOLUTION = get_config_val('GLOFAS_RESOLUTION')
GLOFAS_PROCESSED_THRESH_FILENAME = get_config_val("GLOFAS_PROCESSED_THRESH_FILENAME")
S3_GLOFAS_FILTERED_PATH = get_config_val("S3_GLOFAS_FILTERED_PATH")
S3_GLOFAS_PROCESSED_PATH = get_config_val("S3_GLOFAS_PROCESSED_PATH")
GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME = get_config_val("GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME")
GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME = get_config_val("GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME")

GLOFAS_FLOOD_TENDENCIES = get_config_val('GLOFAS_FLOOD_TENDENCIES')
GLOFAS_FLOOD_INTENSITIES = get_config_val('GLOFAS_FLOOD_INTENSITIES')
GLOFAS_FLOOD_PEAK_TIMINGS = get_config_val('GLOFAS_FLOOD_PEAK_TIMINGS')

USE_FIRST_AS_CONTROL = get_config_val('USE_FIRST_AS_CONTROL')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Define parameters to read the dataframes**

# COMMAND ----------

date = datetime.utcnow() #- timedelta(days=3)
formatted_date = date.strftime("%Y-%m-%d")
formatted_date

# COMMAND ----------

# Create UDF for rounding latitude and longitude
# to ensure joining on these values is successful
round_udf = create_round_udf(GLOFAS_PRECISION)

# COMMAND ----------

CustomSchemaWithoutTimestamp = StructType([
    StructField("number", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("time", LongType(), True),
    StructField("step", LongType(), True),
    StructField("valid_time", LongType(), True),
    StructField("dis24", FloatType(), True)
])

# COMMAND ----------

filtered_wildcard = 'filtered-*.parquet'
#a_couple_files = 'filtered-24*.parquet'
processed_discharge_filepath = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_FILTERED_PATH, formatted_date, filtered_wildcard)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Read the GloFAS forecast data**

# COMMAND ----------

# Load all the forecast data from a folder into a single dataframe
all_forecasts_df = spark.read.schema(CustomSchemaWithoutTimestamp)\
                        .parquet(processed_discharge_filepath)\
                        .withColumn("latitude", round_udf("latitude"))\
                        .withColumn("longitude", round_udf("longitude"))\
                        .withColumn("time", F.to_date(F.to_timestamp(F.col("time") / 1e9)))\
                        .withColumn("valid_time", F.to_date(F.to_timestamp(F.col("valid_time") / 1e9)))\
                        .withColumn("step", (F.col("step") / (60 * 60 * 24 * 1e9)).cast("int"))

# COMMAND ----------

# Repartition dataframe to group by unique latitude and longitude pairs, 
# optimizing join operations on spatial coordinates.
all_forecasts_df = all_forecasts_df.repartition(100, "latitude", "longitude")

# COMMAND ----------

# Read and broadcast the threshold dataframe
threshold_file_path = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, 
                                   GLOFAS_PROCESSED_THRESH_FILENAME)
# Round the lat/lon columns as a safety measure
# although it is assumed to already have been
# done in the threshold joining operation
threshold_df = spark.read.parquet(threshold_file_path)\
                         .withColumn("latitude", round_udf("latitude"))\
                         .withColumn("longitude", round_udf("longitude"))

# COMMAND ----------

# Broadcast thresholds as it is joined to forecast dataframe
# threshold_df = F.broadcast(threshold_df)

# Repartitioning might be better than broadcasting
threshold_df = threshold_df.repartition(100, "latitude", "longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Compute the detailed forecast and cache it**

# COMMAND ----------

detailed_forecast_df = compute_flood_threshold_percentages(all_forecasts_df, 
                                                           threshold_df, 
                                                           GLOFAS_RET_PRD_THRESH_VALS, 
                                                           accuracy_mode='approx')
detailed_forecast_df = detailed_forecast_df.cache()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Load the control forecast**

# COMMAND ----------

if USE_FIRST_AS_CONTROL:
    control_df = detailed_forecast_df.filter(F.col("step") == 1)\
                                     .select("latitude", "longitude", "median_dis")\
                                     .withColumnRenamed("median_dis", "control_dis")
    print('Used first forecast as control')
else:
    control_file_path = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_FILTERED_PATH, formatted_date, 'control.parquet')
    control_df = spark.read.schema(CustomSchemaWithoutTimestamp)\
                        .parquet(control_file_path)\
                        .withColumn("latitude", round_udf("latitude"))\
                        .withColumn("longitude", round_udf("longitude"))\
                        .drop('step')\
                        .withColumnRenamed('dis24', 'control_dis')\
                        .drop('number')\
                        .drop('time')\
                        .drop('valid_time')
                        # Control dataframe's time and valid_time aren't necessary
                        # .withColumn("control_time", F.to_timestamp(F.col("time") / 1e9)).drop('time')\
                        # .withColumn("control_valid_time", F.to_timestamp(F.col("valid_time") / 1e9)).drop('valid_time')\
    print('Used GloFAS control')

# COMMAND ----------

# Broadcast thresholds as it is joined to detailed forecast dataframe
# control_df = F.broadcast(control_df)

# Repartitioning might be better than broadcasting
control_df = control_df.repartition(100, "latitude", "longitude")

# COMMAND ----------

# Add control discharge to the detailed forecast
detailed_with_control_df = detailed_forecast_df.join(control_df, 
                                                     on=['latitude', 'longitude'], 
                                                     how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create the forecast dataframes of interest**

# COMMAND ----------

# Compute the summary forecast values
tendency_df = compute_flood_tendency(detailed_with_control_df, GLOFAS_FLOOD_TENDENCIES, col_name='tendency')
intensity_df = compute_flood_intensity(detailed_forecast_df, GLOFAS_FLOOD_INTENSITIES, col_name='intensity')
peak_timing_df = compute_flood_peak_timing(detailed_forecast_df, GLOFAS_FLOOD_PEAK_TIMINGS, col_name='peak_timing')

# COMMAND ----------

# Join the three tables together to create a single summary dataframe
tendency_and_intensity_df = tendency_df.join(intensity_df, on=['latitude', 'longitude'])
summary_forecast_df = peak_timing_df.join(tendency_and_intensity_df, on=['latitude', 'longitude'])

# COMMAND ----------

# Add the grid geometry to the forecast dataframes 
# for simple creation geometry column in geopandas
summary_forecast_df = add_geometry(summary_forecast_df, GLOFAS_RESOLUTION / 2, round_udf)
detailed_forecast_df = add_geometry(detailed_forecast_df, GLOFAS_RESOLUTION / 2, round_udf)

# COMMAND ----------

# Restrict summary forecast to only the cells that
# have a relevant flood forecast (no 'Gray' intensity)
summary_forecast_df = summary_forecast_df.filter(
    summary_forecast_df['intensity'] != GLOFAS_FLOOD_INTENSITIES['gray'])
# Filter the detailed forecast with the idenitified
# grid cells identified in the summary dataframe
detailed_forecast_df = detailed_forecast_df.join(
    summary_forecast_df.select(['latitude', 'longitude']), 
    on=["latitude", "longitude"], how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Defining paths**

# COMMAND ----------

# Create folder on cloud storage
target_folder = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_PROCESSED_PATH, formatted_date)
dbutils.fs.mkdirs(target_folder)

# Define summary forecast file path
summary_forecast_file_path = os.path.join(target_folder, GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME)
print(summary_forecast_file_path)

# Define detailed forecast file path
detailed_forecast_file_path = os.path.join(target_folder, GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME)
print(detailed_forecast_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Write to mounted cloud storage (and database?)**

# COMMAND ----------

summary_forecast_df.write.parquet(summary_forecast_file_path)

# COMMAND ----------

detailed_forecast_df.write.parquet(detailed_forecast_file_path)

# COMMAND ----------

# If the processed data should be written to a PostgreSQL database, 
# this could be a way of achieving that. If writing to both cloud 
# storage and database, a .cache() call should be made on both the
# summary and detailed forecasts before writing to cloud storage to
# avoid recomputing when writing to database.

# db_properties = {
#     "user": "your_username",
#     "password": "your_password",
#     "driver": "org.postgresql.Driver"
# }

# db_url = "jdbc:postgresql://your_host:your_port/your_database_name"

# COMMAND ----------

# summary_forecast_df.write.jdbc(url=db_url, table="summary_forecast", 
#                                mode="overwrite", properties=db_properties)

# COMMAND ----------

# detailed_forecast_df.write.jdbc(url=db_url, table="detailed_forecast", 
#                                 mode="overwrite", properties=db_properties)
