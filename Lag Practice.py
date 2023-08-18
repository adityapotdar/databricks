# Databricks notebook source
data = [
    (1, '2023-06-01', 10),
    (2, '2023-06-02', 25),
    (3, '2023-06-03', 20),
    (4, '2023-06-04', 30)
]

# COMMAND ----------

df = spark.createDataFrame(data, ["id", "date", "temp"])
display(df)

# COMMAND ----------

df1 = df.withColumn("date", df["date"].cast("date"))
display(df1)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import lag

windowSpec = Window.orderBy("date")
df2 = df1.withColumn("prev_temp", lag("temp").over(windowSpec))
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col, when

df3 = df2.withColumn("currenttemGreate", col("temp") > col("prev_temp"))

# df3 = df2.withColumn("currenttemGreate", when(col("temp") > col("prev_temp"), "true").when(col("temp") < col("prev_temp"), "false").otherwise("null"))
display(df3)