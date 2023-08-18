# Databricks notebook source
count = spark.sparkContext.accumulator(0)

# COMMAND ----------

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd.foreach(lambda x: count.add(1))

# COMMAND ----------

count.value
