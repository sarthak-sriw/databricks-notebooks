# Databricks notebook source
# MAGIC %run "/Users/sriw3999@outlook.com/config_notebook"

# COMMAND ----------

df_agent = spark.read.csv(source_path_agent,header=True,inferSchema=True)
display(df_agent)

# COMMAND ----------

df_agent.dtypes

# COMMAND ----------

df_personal = spark.read.csv(source_path_personal,header=True,inferSchema=True)
display(df_personal)

# COMMAND ----------

join_df = df_agent.join(df_personal,df_agent.AGENTCODE == df_personal.AGENT_CODE ,"outer")
display(join_df)

# COMMAND ----------

#semi
join_df = df_agent.join(df_personal,df_agent.AGENTCODE == df_personal.AGENT_CODE ,"semi")
display(join_df)

# COMMAND ----------

#anti
join_df = df_agent.join(df_personal,df_agent.AGENTCODE == df_personal.AGENT_CODE ,"anti")
display(join_df)

# COMMAND ----------

#cross
display(df_agent.crossJoin(df_personal))

# COMMAND ----------

