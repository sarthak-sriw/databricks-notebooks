# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType ,StringType ,DoubleType

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



df = spark.read.option("header",True).schema(df_schema).csv("dbfs:/FileStore/tables/agent")
display(df)

# COMMAND ----------

