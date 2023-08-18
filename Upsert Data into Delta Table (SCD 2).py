# Databricks notebook source
from delta.tables import *

DeltaTable.createOrReplace(spark) \
    .tableName("scd2Demo") \
    .addColumn("pk1", "INT") \
    .addColumn("pk2", "STRING") \
    .addColumn("dim1", "INT") \
    .addColumn("dim2", "INT") \
    .addColumn("dim3", "INT") \
    .addColumn("dim4", "INT") \
    .addColumn("active_status", "STRING") \
    .addColumn("start_date", "TIMESTAMP") \
    .addColumn("end_date", "TIMESTAMP") \
    .location("/FileStore/tables/scd2Demo") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO scd2demo VALUES (111, 'Unit1', 200, 500, 800, 400, 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO scd2demo VALUES (222, 'Unit2', 900, Null, 700, 100, 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO scd2demo VALUES (333, 'Unit3', 300, 900, 250, 650, 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

from delta.tables import *

targetTable = DeltaTable.forName(spark, "scd2demo")
targetDF = targetTable.toDF()
display(targetDF)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    StructField("pk1", StringType(), True),
    StructField("pk2", StringType(), True),
    StructField("dim1", IntegerType(), True),
    StructField("dim2", IntegerType(), True),
    StructField("dim3", IntegerType(), True),
    StructField("dim4", IntegerType(), True)
])

# COMMAND ----------

data = [
    (111, 'Unit1', 200, 500, 800, 400),
    (222,"Unit2", 800, 1300, 800, 500),
    (444, "Unit4", 100, None, 700, 300)
]

sourceDF = spark.createDataFrame(data = data, schema = schema)
display(sourceDF)

# COMMAND ----------

joinedDF = sourceDF.join(targetDF, (sourceDF.pk1 == targetDF.pk1) & (sourceDF.pk2 == targetDF.pk2) & (targetDF.active_status == 'Y'), "left") \
    .select(sourceDF["*"], targetDF.pk1.alias("target_pk1"), targetDF.pk2.alias("target_pk2"), targetDF.dim1.alias("target_dim1"), targetDF.dim2.alias("target_dim2"), targetDF.dim3.alias("target_dim3"), targetDF.dim4.alias("target_dim4"))
display(joinedDF)

# COMMAND ----------

from pyspark.sql.functions import concat

filterDF = joinedDF.filter(xxhash64(joinedDF.dim1, joinedDF.dim2, joinedDF.dim3, joinedDF.dim4) != xxhash64(joinedDF.target_dim1, joinedDF.target_dim2, joinedDF.target_dim3, joinedDF.target_dim4))

display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY", concat(filterDF.pk1, filterDF.pk2))
display(mergeDF)

# COMMAND ----------

dummyDF = filterDF.filter("target_pk1 is not null").withColumn("MERGEKEY", lit(None))
display(dummyDF)

# COMMAND ----------

scdDF = mergeDF.union(dummyDF)
display(scdDF)

# COMMAND ----------

targetTable.alias("target").merge(
    source = scdDF.alias("source"),
    condition = "concat(target.pk1, target.pk2) = source.MERGEKEY and target.active_status = 'Y'"
).whenMatchedUpdate(
    set = {
        "active_status" : "'N'",
        "end_date" : "current_date"
    }
).whenNotMatchedInsert(
    values = {
        "pk1" : "source.pk1",
        "pk2" : "source.pk2",
        "dim1" : "source.dim1",
        "dim2" : "source.dim2",
        "dim3" : "source.dim3",
        "dim4" : "source.dim4",
        "active_status" : "'Y'",
        "start_date" : 'current_date',
        "end_date" : "to_date('9999-12-31', 'yyyy-MM-dd')"
    }
).execute()