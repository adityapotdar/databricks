# Databricks notebook source
from delta.tables import *

DeltaTable.createOrReplace(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT") \
    .addColumn("Dept", "STRING") \
    .property("description", "This is for demo purpose") \
    .location("/FileStore/tables/delta/employee_db") \
    .execute()

# COMMAND ----------

# DBTITLE 1,SQL Style Insert
# MAGIC %sql
# MAGIC INSERT INTO employee_demo VALUES (100, "Aditya", "M", 10000, "IT")

# COMMAND ----------

# DBTITLE 1,DataFrame Insert
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

emp_data = [(200, "Praj", "F", 12000, "SNOW")]

employee_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("emp_name", StringType(), True),
    StructField("gender", StringType(), False),
    StructField("salary", IntegerType(), True),
    StructField("Dept", StringType(), True)
])

df = spark.createDataFrame(data = emp_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

# DBTITLE 1,DataFrame Insert Into Method
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

employee_data = [(300, "Lara", "F", 6000, "SALES")]

employee_schema= StructType([
    StructField("emp_id", IntegerType(), False), \
    StructField("emp_name", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True), \
    StructField("dept", StringType(), True) \
])

df1 = spark.createDataFrame(data = employee_data, schema = employee_schema)
display (df1)

# COMMAND ----------

df1.write.insertInto("employee_demo", False)