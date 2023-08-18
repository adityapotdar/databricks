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

# MAGIC %sql
# MAGIC
# MAGIC insert into employee_demo values (100, "Stephen", "M", 2000, "IT");
# MAGIC insert into employee_demo values (200, "Philipp", "M", 8000,"HR");
# MAGIC insert into employee_demo values (300, "Lara", "F", 6000, "SALES");
# MAGIC insert into employee_demo values (400, "Mike", "M", 4000, "IT");
# MAGIC insert into employee_demo values (500, "Sarah", "F", 9000, "HR");
# MAGIC insert into employee_demo values (600, "Serena", "F", 5000, "SALES");
# MAGIC insert into employee_demo values (700, "Mark", "M", 7000, "SALES");

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM employee_demo where emp_id = 100

# COMMAND ----------

# DBTITLE 1,PySpark Delta Table Instance
from delta.tables import *

employee_instance = DeltaTable.forName(spark, "employee_demo")
employee_instance.delete("emp_id = 200")

# COMMAND ----------

employee_instance.delete("emp_id = 600 and gender = 'F'")

# COMMAND ----------

from pyspark.sql.functions import col

employee_instance.delete(col("emp_id") == 300)