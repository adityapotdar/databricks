# Databricks notebook source
df = spark.read.option("delimiter", "|").option("header", "True").csv("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/input-4.txt")
display(df)

# COMMAND ----------

from pyspark.sql.functions import split, explode_outer, col, posexplode_outer

finalDf = df.select("*", posexplode_outer(split("Education", ","))).drop("Education")
display(finalDf)

# COMMAND ----------

