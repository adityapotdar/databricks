# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY scd2demo

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2023-07-28T04:43:32.000+0000").table("scd2demo")
display(df)

# COMMAND ----------

df1 = spark.read.format("delta").option("versionAsOf", "3").table("scd2demo")
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM scd2demo TIMESTAMP AS OF '2023-07-28T04:43:32.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM scd2demo VERSION AS OF "3"