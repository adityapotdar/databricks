# Databricks notebook source
df = spark.createDataFrame(sc.parallelize([['ABC', [1,2,3]], ['XYZ', [2, None, 4]], ['KLM', [8,7]], ['IJK', [5]]]), ["key", "value"])
df.display()

# COMMAND ----------

df.select("key", df.value[0], df.value[1], df.value[2]).display()

# COMMAND ----------

from pyspark.sql.functions import size, col

dfSize = df.select("key", "value", size("value").alias("size"))
display(dfSize)

# COMMAND ----------

maxValue = dfSize.agg({"size": "max"}).collect()[0][0]
display(maxValue)

# COMMAND ----------

def arraySplitIntoCalls(df, size):
    for i in range(size):
        df = df.withColumn(f"new_col_{i}", df.value[i])
    return df

# COMMAND ----------

x = arraySplitIntoCalls(df, maxValue)
display(x)