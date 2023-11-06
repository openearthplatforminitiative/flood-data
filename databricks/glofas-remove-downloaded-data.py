# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Remove downloaded and intermediate data for forecast computation
# MAGIC
# MAGIC **Should be performed upon successful completion of flood pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Imports**

# COMMAND ----------

import os
import shutil
from tqdm import tqdm
from flood.utils.config import get_config_val

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls /dbfs/mnt/openepi-storage/glofas/processed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch configuration parameters**

# COMMAND ----------

PYTHON_PREFIX = get_config_val("PYTHON_PREFIX")
DBUTILS_PREFIX = get_config_val("DBUTILS_PREFIX")

S3_GLOFAS_DOWNLOADS_PATH = get_config_val("S3_GLOFAS_DOWNLOADS_PATH")
S3_GLOFAS_FILTERED_PATH = get_config_val("S3_GLOFAS_FILTERED_PATH")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Delete all files/folders in the api downloads and filtered data folders**

# COMMAND ----------

downloads_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_DOWNLOADS_PATH)
filtered_path = os.path.join(PYTHON_PREFIX, S3_GLOFAS_FILTERED_PATH)

# COMMAND ----------

print(downloads_path)
print(filtered_path)

# COMMAND ----------

for directory_path in [downloads_path, filtered_path]:
    # Check if the directory exists before trying to delete
    if os.path.exists(directory_path):
        print(f'Deleting files & folders in {directory_path}')
        # Iterate over all the entries in the directory
        for entry in tqdm(os.listdir(directory_path)):
            # Create full path to the entry
            full_path = os.path.join(directory_path, entry)
            # Check if it's a file or directory
            if os.path.isdir(full_path):
                shutil.rmtree(full_path)  # Remove the directory and all its contents
            else:
                os.remove(full_path)  # Remove the file
