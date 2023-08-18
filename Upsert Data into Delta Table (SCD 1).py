# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark) \
    .tableName("employee_details") \
    .addColumn("id", "INT") \
    .addColumn("name", "STRING") \
    .addColumn("salary", "INT") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO employee_details VALUES (1, "Aditya", 100);
# MAGIC INSERT INTO employee_details VALUES (2, "Praj", 100);

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [(3, "Prasad", 1500)]

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("salary", IntegerType())
])

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("sourceData")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO employee_details AS target
# MAGIC USING sourceData AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.id = source.id,
# MAGIC   target.name = source.name,
# MAGIC   target.salary = source.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, name, salary) VALUES (id, name, salary)
# MAGIC

# COMMAND ----------

data1 = [(3, "Prasad", 15), (4, "Vaibhav", 100)]
df1 = spark.createDataFrame(data1, schema)
display(df1)

# COMMAND ----------

df.createOrReplaceTempView("sourceData")

# COMMAND ----------

from delta.tables import *

empTable = DeltaTable.forName(spark, "employee_details")

empTable.alias("target").merge(
    source = df1.alias("source"),
    condition = "source.id == target.id"
).whenMatchedUpdate(
    set = {
        "id" : "source.id",
        "name" : "source.name",
        "salary" : "source.salary"
    }
).whenNotMatchedInsert(
    values = {
        "id" : "source.id",
        "name" : "source.name",
        "salary" : "source.salary"
    }
).execute()