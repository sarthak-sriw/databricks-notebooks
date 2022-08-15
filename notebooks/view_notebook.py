# Databricks notebook source
df = spark.read.parquet("dbfs:/FileStore/tables/unloads_path/processed")
display(df)

# COMMAND ----------

df.createOrReplaceTempView("V_agent")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from V_agent

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

display(spark.sql("select * from V_agent"))

# COMMAND ----------

df.createOrReplaceGlobalTempView("gv_agent")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from global_temp.gv_agent

# COMMAND ----------

display(spark.sql("select * from global_temp.gv_agent"))

# COMMAND ----------

