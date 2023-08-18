# Databricks notebook source
df = spark.read.csv("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/input-2.txt").withColumnRenamed("_c0", "value")
display(df)

# COMMAND ----------

from pyspark.sql.functions import split, explode

df1 = df.select(explode(split(df.value, " ")).alias("val"))
display(df1)

# COMMAND ----------

display(df1.groupBy("val").count())

# COMMAND ----------

sc = spark.sparkContext
rdd = sc.textFile("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/input-2.txt")

# COMMAND ----------

rdd1 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))
rdd2 = rdd1.reduceByKey(lambda x, y: (x + y))
rdd2.collect()

# COMMAND ----------

