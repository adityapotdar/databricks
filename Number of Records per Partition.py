# Databricks notebook source
df = spark.read.format("csv").option("header", "true").load("/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv")
df.show()

# COMMAND ----------

print(df.rdd.getNumPartitions())

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

display(df.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count())

# COMMAND ----------

df1 = df.repartition(5)
print(df1.rdd.getNumPartitions())

# COMMAND ----------

df1.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().show()

# COMMAND ----------

from pyspark.sql.functions import col, count, when

df2 = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
df2.show()

# COMMAND ----------

