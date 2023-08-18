# Databricks notebook source
data = [
    ("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")
]

df = spark.createDataFrame(data, ["Product", "Amount", "Country"])
display(df)

# COMMAND ----------

# DBTITLE 1,Pivot
df1 = df.groupBy("Product").pivot("Country").sum("Amount")
display(df1)

# COMMAND ----------

# DBTITLE 1,Unpivot
from pyspark.sql.functions import expr

df2 = df1.select("Product", expr("stack(4, 'Canada', Canada, 'China', China, 'Mexico', Mexico, 'USA', USA) as (Country, Total)")).where("total is not null")

display(df2)