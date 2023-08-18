# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employee_demo (
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   Dept STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/tables/delta/employee_db'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values (100, "Stephen", "M", 2000, "IT");
# MAGIC insert into employee_demo values (200, "Philipp", "M", 8000,"HR");
# MAGIC insert into employee_demo values (300, "Lara", "F", 6000, "SALES");
# MAGIC insert into employee_demo values (400, "Mike", "M", 4000,"IT");
# MAGIC insert into employee_demo values (500, "Sarah", "F", 9000, "HR");
# MAGIC insert into employee_demo values (600, "Serena", "F", 5000, "SALES");
# MAGIC insert into employee_demo values (700, "Mark", "M", 7000, "SALES");

# COMMAND ----------

# DBTITLE 1,Update using standard SQL
# MAGIC %sql
# MAGIC
# MAGIC UPDATE employee_demo SET salary = 1500 WHERE emp_id = 100

# COMMAND ----------

# DBTITLE 1,PySpark using Table Instance
from delta.tables import *

employee_instance = DeltaTable.forName(spark, "employee_demo")

employee_instance.update(
    condition = "emp_name == 'Lara'",
    set = {"salary" : "1000"}
)

# COMMAND ----------

from pyspark.sql.functions import col, lit

employee_instance.update(
    condition = col("emp_name") == "Stephen",
    set = {"salary": lit("1")}
)