-- Databricks notebook source
show tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("dbfs:/FileStore/tables/agent.csv",inferSchema=True,header=True)
-- MAGIC display(df)

-- COMMAND ----------

create database if not exists test

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE test

-- COMMAND ----------

use test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").saveAsTable("test.agentname")

-- COMMAND ----------

select * from test.agentname

-- COMMAND ----------

DESCRIBE test.agentname

-- COMMAND ----------


DESCRIBE EXTENDED test.agentname

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").option("path","dbfs:/FileStore/tables/agent_external").saveAsTable("test.agent_external")

-- COMMAND ----------

select * from test.agent_external

-- COMMAND ----------

create table if not exists test.agenttable_sql
(
AGENT_CODE string,
AGENT_NAME string,
WORKINGAREA string,
COMMISSION double,
PHONE_NO string,
SALARY int,
EXPERIENCE int
)
using parquet
location "dbfs:/FileStore/tables/agenttable_sql"


-- COMMAND ----------

insert into test.agenttable_sql (AGENT_CODE,AGENT_NAME,WORKINGAREA,COMMISSION,PHONE_NO,SALARY,EXPERIENCE)  
select AGENTCODE,
AGENTNAME,
WORKINGAREA,
COMMISSION,
PHONENO,
SALARY,
EXPERIENCE from test.agentname

-- COMMAND ----------

select count(*) from test.agenttable_sql

-- COMMAND ----------

drop table test.agenttable_sql

-- COMMAND ----------

create table if not exists test.agenttable_sql
(
AGENT_CODE string,
AGENT_NAME string,
WORKINGAREA string,
COMMISSION double,
PHONE_NO string,
SALARY int,
EXPERIENCE int
)
using parquet
location "dbfs:/FileStore/tables/agenttable_sql"

-- COMMAND ----------

select count(*) from test.agenttable_sql

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC dbutils.fs.rm("dbfs:/FileStore/tables/agenttable_sql",True)

-- COMMAND ----------

