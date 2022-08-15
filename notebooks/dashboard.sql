-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Agent DataSet Analysis</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("dbfs:/FileStore/tables/agent.csv",inferSchema=True,header=True)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_1 = spark.read.csv("dbfs:/FileStore/tables/agent.csv",inferSchema=True,header=True)
-- MAGIC display(df_1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_2 = spark.read.csv("dbfs:/FileStore/tables/agent.csv",inferSchema=True,header=True)
-- MAGIC display(df_2)

-- COMMAND ----------

