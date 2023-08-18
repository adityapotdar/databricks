# Databricks notebook source
df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/corrupt-2.csv")
df.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Month", StringType()),
    StructField("Emp_count", IntegerType()),
    StructField("Production_unit", IntegerType()),
    StructField("Expense", IntegerType()),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Permissive Mode
df1 = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", "true").schema(schema).load("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/corrupt-2.csv")

df1.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Drop Malformed
df2 = spark.read.format("csv").option("header", "true").option("mode","DROPMALFORMED").schema(schema).load("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/corrupt-2.csv")
df2.show(truncate = False)

# COMMAND ----------

# DBTITLE 1,Fail Fast
df3 = spark.read.format("csv").option("header", "true").option("mode", "FAILFAST").schema(schema).load("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/corrupt-2.csv")
display(df3)
