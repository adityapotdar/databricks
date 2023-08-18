# Databricks notebook source
df = spark.read.format("csv").option("header", "True").option("delimiter", "|").load("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/input-6.txt")
display(df)

# COMMAND ----------

from pyspark.sql import Window

windowSpec = Window.partitionBy("Sub").orderBy(df["Marks"].desc())

# COMMAND ----------

from pyspark.sql.functions import rank, dense_rank

df1 = df.withColumn("rank", dense_rank().over(windowSpec)).filter("rank = 1")
display(df1)

# COMMAND ----------

df.createOrReplaceTempView("d")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM (
# MAGIC   SELECT *, RANK() OVER(PARTITION BY Sub ORDER BY Marks desc) as rank
# MAGIC   FROM d)
# MAGIC WHERE rank = 1

# COMMAND ----------

