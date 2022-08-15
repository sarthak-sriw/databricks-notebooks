# Databricks notebook source
# MAGIC %run "/Users/sriw3999@outlook.com/config_notebook"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType ,StringType ,DoubleType

# COMMAND ----------

df = spark.read.csv(source_path,header=True,inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_schema = StructType(fields=[StructField("AGENTCODE",StringType(),False),
                              StructField("AGENTNAME",StringType(),True),
                               StructField("WORKINGAREA",StringType(),True),
                               StructField("COMMISSION",DoubleType(),True),
                               StructField("PHONENO",StringType(),True),
                               StructField("SALARY",IntegerType(),True),
                              StructField("EXPERIENCE",IntegerType(),True)                              
                              ])

# COMMAND ----------

df1 = spark.read.option("header",True).schema(df_schema).csv(source_path)

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

selected_df1 = df1.select(col("AGENTCODE"),col("AGENTNAME"),col("WORKINGAREA"),col("COMMISSION"),col("PHONENO"),col("SALARY"),col("EXPERIENCE"))

# COMMAND ----------

display(selected_df1)

# COMMAND ----------

dbutils.widgets.text("datasource","","data_source")
data_source = dbutils.widgets.get("datasource")

# COMMAND ----------

data_source

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

rename_df = selected_df1.withColumnRenamed("AGENTCODE","AGENT_CODE").withColumnRenamed("AGENTNAME","AGENT_NAME").withColumnRenamed("PHONENO","PHONE_NO").withColumn("data_source",lit(data_source))

# COMMAND ----------

display(rename_df)

# COMMAND ----------

final_df = append_column(rename_df)


# COMMAND ----------

#dbutils.notebook.run("")

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{unload_path}/processed")

# COMMAND ----------

display(spark.read.parquet(f"{unload_path}/processed"))