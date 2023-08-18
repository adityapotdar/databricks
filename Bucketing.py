# Databricks notebook source
# DBTITLE 1,Check if Bucketing is enabled
spark.conf.get("spark.sql.sources.bucketing.enabled")

# COMMAND ----------

from pyspark.sql.functions import col, rand

df = spark.range(1, 10000, 1, 10).select(col("id").alias("pk"), rand(10).alias("attibute"))
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# DBTITLE 1,Create Non Bucketed Table
df.write.format("parquet").saveAsTable("nonBucketedTable")

# COMMAND ----------

# DBTITLE 1,Create Non Bucketed Table
df.write.format("parquet").bucketBy(10, "pk").saveAsTable("bucketedTable")

# COMMAND ----------

df1 = spark.table("bucketedtable")
df2 = spark.table("bucketedtable")

df3 = spark.table("nonbucketedtable")
df4 = spark.table("nonbucketedtable")

# COMMAND ----------

# DBTITLE 1,Broadcast join by default if size less then 10 MB
df3.join(df4, "pk", "inner").explain()

# COMMAND ----------

# DBTITLE 1,Disable Broadcast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", False)

# COMMAND ----------

display(df3.join(df4, "pk", "inner"))

# COMMAND ----------

# DBTITLE 1,Non Bucketed to Non Bucketed Join
df3.join(df4, "pk", "inner").explain()

# COMMAND ----------

df3.join(df1, "pk", "inner").display()

# COMMAND ----------

# DBTITLE 1,Non Bucketed to Bucketed Join. one side would be Shuffled
df3.join(df1, "pk", "inner").explain()

# COMMAND ----------

df1.join(df2, "pk", "inner").display()

# COMMAND ----------

# DBTITLE 1,Bucketed to Bucketed Join. No shuffle invovled
df1.join(df2, "pk", "inner").explain()
