# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Perform threshold processing and joining
# MAGIC
# MAGIC **Only needs to be performed once (done)**
# MAGIC
# MAGIC This guide assumes the converted files `RP2ythresholds_GloFASv40.parquet`, `RP5ythresholds_GloFASv40.parquet`, and `RP20ythresholds_GloFASv40.parquet` have been saved to the auxiliary data folder of the mounted S3 bucket.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os
from flood.utils.config import get_config_val
from flood.spark.transforms import create_round_udf
from pyspark.sql.functions import lit
from functools import reduce

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
        .appName("ThresholdDataJoining") \
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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Perform processing and joining**

# COMMAND ----------

# Create UDF for rounding latitude and longitude
# to endure joining on these values is successful
round_udf = create_round_udf(GLOFAS_PRECISION)

dataframes = []

# Read in dataframes, rename threshold column, add return period column, and round lat/lon values
for threshold in GLOFAS_RET_PRD_THRESH_VALS:

    threshold_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]
    threshold_filepath = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, threshold_filename)

    df = (spark.read.parquet(threshold_filepath)
          .withColumnRenamed(f"{threshold}yRP_GloFASv4", f"{threshold}y_threshold")
          .withColumn("lat", round_udf("lat"))
          .withColumn("lon", round_udf("lon")))
    dataframes.append(df)

# Get a list of counts for each dataframe
counts = [df.count() for df in dataframes]

# Check if all counts are the same
assert len(set(counts)) == 1, f"Not all dataframes have the same count. Counts: {counts}"

# Store the count of one dataframe for later comparison
original_count = counts[0]

# Join the dataframes based on lat and lon
# Assumes the number of dataframes to join is > 1
combined_df = dataframes[0]
for next_df in dataframes[1:]:
    combined_df = combined_df.join(next_df, on=['lat', 'lon'], how='inner')

# Check if the count after joining is still the same
combined_row_count = combined_df.count()
assert combined_row_count == original_count, f"The count after joining ({combined_row_count}) is not the same as before ({original_count})."

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Sort before saving**

# COMMAND ----------

sorted_df = combined_df.sort(["lat", "lon"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Write to mounted S3 bucket**

# COMMAND ----------

target_filepath = os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_PROCESSED_THRESH_FILENAME)
sorted_df.write.parquet(target_filepath)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Check that the files are present in target folder**

# COMMAND ----------

dbutils.fs.ls(os.path.join(DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH))
