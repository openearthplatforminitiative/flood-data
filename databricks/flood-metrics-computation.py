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
# MAGIC ls /dbfs/mnt/openepi-storage/glofas

# COMMAND ----------

import os
from datetime import datetime, timedelta
from flood.utils.config import get_config_val
from flood.spark.transforms import (create_round_udf, 
                                    compute_flood_tendency,
                                    compute_flood_intensity,
                                    compute_flood_peak_timing,
                                    compute_flood_threshold_percentages)
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

# Load all the forecast data from a folder into a single dataframe
all_forecasts_df = spark.read.schema(CustomSchemaWithoutTimestamp)\
                        .parquet(processed_discharge_filepath)\
                        .withColumn("latitude", round_udf("latitude"))\
                        .withColumn("longitude", round_udf("longitude"))\
                        .withColumn("time", F.to_date(F.to_timestamp(F.col("time") / 1e9)))\
                        .withColumn("valid_time", F.to_date(F.to_timestamp(F.col("valid_time") / 1e9)))\
                        .withColumn("step", (F.col("step") / (60 * 60 * 24 * 1e9)).cast("int"))

# COMMAND ----------

all_forecasts_df.show(5)

# COMMAND ----------

# For testing purposes only

# min_test_latitude = 2.1
# max_test_latitude = 2.5
# min_test_longitude = 10.35
# max_test_longitude = 11.2

# all_forecasts_df = all_forecasts_df.filter(
#     (all_forecasts_df.latitude >= min_test_latitude) & 
#     (all_forecasts_df.latitude <= max_test_latitude) & 
#     (all_forecasts_df.longitude >= min_test_longitude) & 
#     (all_forecasts_df.longitude <= max_test_longitude)
# )

# COMMAND ----------

all_forecasts_df = all_forecasts_df.repartition(100, "latitude", "longitude")

# COMMAND ----------

# Read and broadcast the threshold dataframe
threshold_df = spark.read.parquet(os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_PROCESSED_THRESH_FILENAME))

# For testing purposes only
# threshold_df = threshold_df.filter(
#     (threshold_df.latitude >= min_test_latitude) & 
#     (threshold_df.latitude <= max_test_latitude) & 
#     (threshold_df.longitude >= min_test_longitude) & 
#     (threshold_df.longitude <= max_test_longitude)
# )

broadcast_threshold_df = F.broadcast(threshold_df)

# COMMAND ----------

broadcast_threshold_df.show(5)

# COMMAND ----------

detailed_forecast_df = compute_flood_threshold_percentages(all_forecasts_df, 
                                                           broadcast_threshold_df, 
                                                           GLOFAS_RET_PRD_THRESH_VALS, 
                                                           accuracy_mode='approx')

# COMMAND ----------

detailed_forecast_df = detailed_forecast_df.cache()

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

# For testing purposes only
# control_df = control_df.filter(
#     (control_df.latitude >= min_test_latitude) & 
#     (control_df.latitude <= max_test_latitude) & 
#     (control_df.longitude >= min_test_longitude) & 
#     (control_df.longitude <= max_test_longitude)
# )

# COMMAND ----------

detailed_with_control_df = detailed_forecast_df.join(control_df, 
                                                     on=['latitude', 'longitude'], 
                                                     how="left")

# COMMAND ----------

tendency_df = compute_flood_tendency(detailed_with_control_df, GLOFAS_FLOOD_TENDENCIES, col_name='tendency')
intensity_df = compute_flood_intensity(detailed_forecast_df, GLOFAS_FLOOD_INTENSITIES, col_name='intensity')
peak_timing_df = compute_flood_peak_timing(detailed_forecast_df, GLOFAS_FLOOD_PEAK_TIMINGS, col_name='peak_timing')

# COMMAND ----------

tendency_and_intensity_df = tendency_df.join(intensity_df, on=['latitude', 'longitude'])
summary_forecast_df = peak_timing_df.join(tendency_and_intensity_df, on=['latitude', 'longitude'])

# COMMAND ----------

target_folder = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_PROCESSED_PATH, formatted_date)
dbutils.fs.mkdirs(target_folder)
summary_forecast_file_path = os.path.join(target_folder, GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME)
summary_forecast_file_path

# COMMAND ----------

detailed_forecast_file_path = os.path.join(target_folder, GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME)
detailed_forecast_file_path

# COMMAND ----------

summary_forecast_df.write.parquet(summary_forecast_file_path)

# COMMAND ----------

detailed_forecast_df.write.parquet(detailed_forecast_file_path)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Inspect processed data in pandas
import pandas as pd
pandas_summary_df = pd.read_parquet(os.path.join(PYTHON_PREFIX, 
                                         S3_GLOFAS_PROCESSED_PATH, 
                                         formatted_date, 
                                         GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME)
                            )

# COMMAND ----------

pandas_summary_df

# COMMAND ----------

pandas_summary_df[(pandas_summary_df['latitude'] == 2.375) & (pandas_summary_df['longitude'] == 10.525)]

# COMMAND ----------

pandas_summary_df[(pandas_summary_df['latitude'] == 2.425) & (pandas_summary_df['longitude'] == 10.475)]

# COMMAND ----------

pandas_summary_df[(pandas_summary_df['latitude'] == 1.925) & (pandas_summary_df['longitude'] == 10.775)]

# COMMAND ----------

pandas_summary_df[(pandas_summary_df['latitude'] == 0.425) & (pandas_summary_df['longitude'] == 12.175)]

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/openepi-storage/glofas/processed/2023-10-25')

# COMMAND ----------


