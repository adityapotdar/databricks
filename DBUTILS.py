# Databricks notebook source
dbutils.fs.ls("/databricks-datasets/")

# COMMAND ----------

# DBTITLE 1,Notebook command
dbutils.notebook.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/COVID/coronavirusdataset/Case.csv")

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/COVID/coronavirusdataset/Case.csv")