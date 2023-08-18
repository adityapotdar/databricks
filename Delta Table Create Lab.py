# Databricks notebook source
# DBTITLE 1,Method 1 - PySpark
from delta.tables import *

DeltaTable.create(spark) \
    .tableName("employee_demo") \
    .addColumn("id", "INT") \
    .addColumn("name", "STRING") \
    .addColumn("gender", "STRING") \
    .property("description", "table created for demo purpose") \
    .location("/FileStore/tables/delta") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM employee_demo

# COMMAND ----------

from delta.tables import *
DeltaTable.createIfNotExists(spark) \
    .tableName("employee") \
    .addColumn("id", "INT") \
    .addColumn("name", "STRING") \
    .property("description", "Demo") \
    .execute()

# COMMAND ----------

from delta.tables import *
DeltaTable.createOrReplace(spark) \
    .tableName("Employee") \
    .addColumn("id", "INT") \
    .execute()

# COMMAND ----------

# DBTITLE 1,Method 2 - SQL
# MAGIC %sql
# MAGIC CREATE TABLE emp_demo (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   gender STRING
# MAGIC  ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS emp_demo (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE emp_demo (
# MAGIC   if INT,
# MAGIC   name STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION '/FileStore/tables/delta'

# COMMAND ----------

# DBTITLE 1,Method 3 - Using DataFrame
employee_data = [
    (100, "Stephen", "M", 2000, "IT"),
    (200, "Philipp", "M", 8000,"HR"),
    (300, "Lara", "F", 6000, "SALES")]

employee_schema = ["emp_id", "emp_name", "gender","salary","dept"]

df = spark.createDataFrame(data=employee_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.format("delta").saveAsTable("employee_db")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee_db