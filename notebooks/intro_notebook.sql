-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Notebook Introduction
-- MAGIC * SQL
-- MAGIC * python

-- COMMAND ----------

--/FileStore/tables

create table if not exists agent using csv OPTIONS (path="/FileStore/tables/agent.csv",header=true)


-- COMMAND ----------

select * from agent

-- COMMAND ----------

select WORKING_AREA,count(*) from agent group by WORKING_AREA

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Please subscribe to my channel")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.ls('/databricks-datasets/')

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/tables/

-- COMMAND ----------

