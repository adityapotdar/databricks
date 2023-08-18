# Databricks notebook source
df = spark.read.option("header", "True").option("inferSchema", "true").csv("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/input-5.txt")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, when

df1 = df.withColumn("amount_sub", when(col("Transaction Type") == "debit", -1 * col("Amount")).otherwise(col("Amount")))
df2 = df1.groupby("Customer_No").agg(sum("amount_sub"))
display(df2)