# Databricks notebook source
# DBTITLE 1,Array Data
array_appliance = [
    ('Raja', ['TV', 'Refrigerator', 'Oven', 'AC']),
    ('Raghav', ['AC', 'Washing machine',None]),
    ('Ram', ['Grinder', 'TV']),
    ('Ramesh', ['Refrigerator', 'TV', None]),
    ('Rajesh',None)]
df_app = spark.createDataFrame (data=array_appliance, schema= ['name', 'Appliances'])
df_app.printSchema()
display(df_app)

# COMMAND ----------

# DBTITLE 1,Map Data
map_brand= [
    ('Raja', {'TV':'LG', 'Refrigerator': 'Samsung', 'Oven': 'Philipps', 'AC': 'Voltas' }),
    ('Raghav', {'AC': 'Samsung', 'Washing machine': 'LG' }),
    ('Ram', {'Grinder': 'Preethi', 'TV': ''}),
    ('Ramesh', {'Refrigerator':'LG', 'TV': 'Croma' }),
    ('Rajesh', None)]
df_brand = spark.createDataFrame(data=map_brand, schema= ['name', 'Brand'])
df_brand.printSchema()
display(df_brand)

# COMMAND ----------

from pyspark.sql.functions import explode, col

arrayDf = df_app.select("name", explode("Appliances"))
arrayDf.show()

# COMMAND ----------

mapDf = df_brand.select("name", explode("brand"))
mapDf.printSchema()
mapDf.show()

# COMMAND ----------

from pyspark.sql.functions import explode_outer

arrayDf2 = df_app.select("name", explode_outer("appliances"))
arrayDf2.show()

# COMMAND ----------

mapDf = df_brand.select("name", explode_outer("brand"))
mapDf.printSchema()
mapDf.show()

# COMMAND ----------

from pyspark.sql.functions import posexplode

arrayDf2 = df_app.select("name", posexplode("appliances"))
arrayDf2.show()

# COMMAND ----------

mapDf2 = df_brand.select("name", posexplode("brand"))
mapDf2.printSchema()
mapDf2.show()

# COMMAND ----------

from pyspark.sql.functions import posexplode_outer
mapDf3 = df_brand.select("name", posexplode_outer("brand"))
mapDf3.printSchema()
mapDf3.show()

# COMMAND ----------

