# Databricks notebook source
# Setup data lake storage account name
storage_account_name = "storage-name"
# Set up an account access key:
storage_account_access_key = "account-key"

file_location = "wasbs://container-name@account-name.blob.core.windows.net/file-name"
file_type = "parquet"


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
    storage_account_access_key)

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

display(df)

# COMMAND ----------

# Details for Storage Account for Synapse Analytics
synapse_storage_account_name = "storage-name"
synapse_storage_account_access_key = "account-key"

sc._jsc.hadoopConfiguration().set(
  "fs.azure.account.key."+synapse_storage_account_name+".blob.core.windows.net",
  synapse_storage_account_access_key)

# COMMAND ----------

df.write \
    .format("com.databricks.spark.sqldw") \
    .option("url", "jdbc:sqlserver://[Dedicated-SQL-endpoint]:1433;database=database-name")\ (azure-synapse-analytics -> workspace -> properties ->Dedicated-SQL-endpoint)
    .option("user", "[username]")\
    .option("password", "[password]")\
    .option("tempDir", "wasbs://[container]@[account-name].blob.core.windows.net/temp")\
    .option("forwardSparkAzureStorageCredentials","true")\
    .option("dbtable","[schema.table-name]")\
    .mode("overwrite") \
    .save()

# COMMAND ----------


