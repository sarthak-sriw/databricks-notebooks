# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

display(dbutils.fs.head('/FileStore/tables/agent-1.csv'))

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %run "/Users/sriw3999@outlook.com/child_notebook"

# COMMAND ----------

path

# COMMAND ----------

dbutils.notebook.run("./child_notebook",10)

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("input1","","Enter the value")

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

