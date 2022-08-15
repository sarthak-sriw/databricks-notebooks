# Databricks notebook source
df = spark.read.csv("dbfs:/FileStore/tables/agent.csv",inferSchema=True,header=True)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("v_agent")

# COMMAND ----------

# MAGIC %sql
# MAGIC show  tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM V_agent

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables
# MAGIC temp - temporary true
# MAGIC global - true

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists test

# COMMAND ----------

# MAGIC %sql 
# MAGIC use test

# COMMAND ----------

df_1 = spark.read.csv("dbfs:/FileStore/tables/agent.csv",inferSchema=True,header=True)
display(df)

# COMMAND ----------

df_1.write.format("parquet").saveAsTable("test.testtable")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from test.testtable

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop  table test.testtable

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop  database test

# COMMAND ----------

df_2 = spark.read.parquet("dbfs:/FileStore/tables/unloads_path/processed")
display(df_2)

# COMMAND ----------

df_2.write.format("parquet").option("path","dbfs:/FileStore/tables/testtable_external").saveAsTable("test.testtable_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.testtable_external

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC desc extended  test.testtable_external

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists test.testtable_sql
# MAGIC (
# MAGIC AGENT_CODE string,
# MAGIC AGENT_NAME string,
# MAGIC WORKINGAREA string,
# MAGIC COMMISSION double,
# MAGIC PHONE_NO string,
# MAGIC SALARY int,
# MAGIC EXPERIENCE int
# MAGIC )
# MAGIC using parquet
# MAGIC location "dbfs:/FileStore/tables/testtable_sql"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe  test.testtable_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into test.testtable_sql (AGENT_CODE,AGENT_NAME,WORKINGAREA,COMMISSION,PHONE_NO,SALARY,EXPERIENCE)  select AGENTCODE,
# MAGIC AGENTNAME,
# MAGIC WORKINGAREA,
# MAGIC COMMISSION,
# MAGIC PHONENO,
# MAGIC SALARY,
# MAGIC EXPERIENCE from test.testtable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.testtable_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC only metadata is deleted not data
# MAGIC drop table test.testtable_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists test.testtable_sql
# MAGIC (
# MAGIC AGENT_CODE string,
# MAGIC AGENT_NAME string,
# MAGIC WORKINGAREA string,
# MAGIC COMMISSION double,
# MAGIC PHONE_NO string,
# MAGIC SALARY int,
# MAGIC EXPERIENCE int
# MAGIC )
# MAGIC using parquet
# MAGIC location "dbfs:/FileStore/tables/testtable_sql"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from test.testtable_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view test.v_testtablesql
# MAGIC as 
# MAGIC select * from test.testtable_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.v_testtablesql

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables
# MAGIC temp - temporary true

# COMMAND ----------

# MAGIC %sql
# MAGIC drop  database  test CASCADE

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/testtable_sql",True)

# COMMAND ----------

