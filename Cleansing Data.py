# Databricks notebook source
df = spark.read.format("text").load("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/input-3.txt").withColumnRenamed("_c0", "value")
display(df)

# COMMAND ----------

headerDf = df.first()[0]
display(headerDf)
dataDf = df.filter(df['value'] != headerDf)
display(dataDf)

# COMMAND ----------

from pyspark.sql.functions import split, regexp_replace

df_temp = dataDf.withColumn("Name", split("value", "~")[0]).withColumn("Age", split("value", "~")[1]).drop("value")
final_df = df_temp.select("Name", regexp_replace("Age", "[|]", "").alias("Age"))

display(final_df)

# COMMAND ----------

