# Databricks notebook source
print("Welcome to my channel")

# COMMAND ----------

#dbutils.notebook.exit(100)

# COMMAND ----------

source_path = "dbfs:/FileStore/tables/agent.csv"

# COMMAND ----------

unload_path = "dbfs:/FileStore/tables/unloads_path"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def append_column(df):
    return df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

source_path_agent = "dbfs:/FileStore/tables/agent.csv"

# COMMAND ----------

source_path_personal = "dbfs:/FileStore/tables/personal.csv"