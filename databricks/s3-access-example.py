# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount S3 bucket to DBFS
# MAGIC Clusters trying to access the mount location must be configured with an [instance profile](https://docs.databricks.com/en/aws/iam/instance-profile-tutorial.html#manage-instance-profiles) with access to the S3 bucket.

# COMMAND ----------

aws_bucket_name = "databricks-data-openepi"
mount_name = "openepi-storage"

# COMMAND ----------

dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access S3 without mounting
# MAGIC The S3 bucket can altertively be accessed without mounting, as long as the cluster is configured with an instance profile.
# MAGIC Files resulting from Python code (e.g. from urlretrieve or requests) will by default be saved on the block storage volume attached to the driver node.
# MAGIC The files can then be moved to the S3 bucket with `dbutils`.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp

# COMMAND ----------

# DBTITLE 1,Create a dummy file with Python
with open("/tmp/s3_test_file.txt", "w") as f:
    f.write("This is a test of S3 access in Databricks.")

# COMMAND ----------

# DBTITLE 1,Move the file to S3
dbutils.fs.mv("file:/tmp/s3_test_file.txt", f"s3a://{aws_bucket_name}/")

# COMMAND ----------

dbutils.fs.ls(f"s3a://{aws_bucket_name}/")

# COMMAND ----------

dbutils.fs.rm(f"s3a://{aws_bucket_name}/s3_test_file.txt")

# COMMAND ----------


