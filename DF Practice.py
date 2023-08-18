# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

schema = StructType([
    StructField("location", StringType(), True),
    StructField("addressProperties", MapType(StringType(), ArrayType(MapType(StringType(), StringType()))))
])

# COMMAND ----------

dataDictionary = [
    ('12345',{"addressAttributes": [{"name": "houseNumber", "value": "718"}, {"name": "streetName", "value": "VIENNA"}, {"name": "streetSuffix", "value": "ST"}, {"name": "city", "value": "METROPOLIS"}, {"name": "state", "value": "IL"}, {"name": "zip5", "value": "62960"}, {"name": "zip4", "value": "1642"}, {"name": "country", "value": "USA"}]}),
    ('678910',{"addressAttributes": [{"name": "houseNumber", "value": "245"}, {"name": "streetName", "value": "LONGVIEW"}, {"name": "streetSuffix", "value": "DR"}, {"name": "city", "value": "PADUCAH"}, {"name": "state", "value": "KY"}, {"name": "zip5", "value": "42001"}, {"name": "zip4", "value": "5968"}, {"name": "country", "value": "USA"}]})
]

# COMMAND ----------

df = spark.createDataFrame(dataDictionary, schema)
display(df)

# COMMAND ----------

df.select(df.addressProperties["addressAttributes"]).show()

# COMMAND ----------

def find(data):
    for i in data:
        if i["name"] == "state":
            return i["value"]

# COMMAND ----------

convertUDF = udf(lambda x: find(x))

# COMMAND ----------

df1 = df.withColumn("state", convertUDF(df.addressProperties["addressAttributes"]))

# COMMAND ----------

df1.drop("addressProperties").show()