# Databricks notebook source
data_student = [
    ("Michael","Science", 80,"p",90),
    ("Nancy", "Mathematics", 90, "p", None),
    ("David", "English", 20, "F",80),
    ("John", "Science", None, "F", None),
    ("Blessy", None, 30, "F", 50),
    ("Martin", "Mathematics", None, None, 70)]

schema = ["name", "subject", "marks", "status", "attendance"]
df = spark.createDataFrame(data_student, schema)
display(df)

# COMMAND ----------

# DBTITLE 1,isNull()
from pyspark.sql.functions import col

display(df.filter(col("marks").isNull()))

# COMMAND ----------

# DBTITLE 1,isNotNull()
display(df.filter((col("marks").isNotNull()) & (col("attendance").isNotNull())))