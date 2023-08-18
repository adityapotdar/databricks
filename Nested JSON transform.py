# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

schema = StructType([
    StructField("Employee", ArrayType(
        StructType([
            StructField("emp_id", IntegerType()),
            StructField("Designation", StringType()),
            StructField("attribute", ArrayType(
                StructType([
                    StructField("Parent_id", IntegerType()),
                    StructField("status_flag", IntegerType()),
                    StructField("Department", ArrayType(
                        StructType([
                            StructField("Dept_id", IntegerType()),
                            StructField("Code", StringType()),
                            StructField("dept_type", StringType()),
                            StructField("dept_flag", IntegerType())
                        ])
                    ))
                ])
            ))
        ])
    ))
])

df = spark.read.format("json").option("multiline", "true").schema(schema).load("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/data-2.json")
df.show()

# COMMAND ----------

from pyspark.sql.functions import explode

df1 = df.select("Employee", explode("Employee").alias("EmployeeExplode")).select("EmployeeExplode.*").select("emp_id", "Designation", explode("attribute").alias("AttibuteExplode")).select("emp_id", "Designation", "AttibuteExplode.*").select("emp_id", "Designation", "Parent_id", "status_flag", explode("Department").alias("departmentexplode")).select("emp_id", "Designation", "Parent_id", "status_flag", "departmentexplode.*")
df1.show()

# COMMAND ----------

