# Databricks notebook source
data_student = [
    ("Raja", "Science", 80, "P",90),
    ("Rakesh", "Maths", 90, "P", 70),
    ("Rama", "English", 20, "F",80),
    ("Ramesh", "Science", 45, "F", 75),
    ("Rajesh", "Maths",30, "F",50),
    ("Raghav", "Maths", None, "NA", 70)]

Schema = ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame(data = data_student, schema = Schema)
display (df)

# COMMAND ----------

from pyspark.sql.functions import when, col

df1 = df.withColumn("Status", when(col("mark") >= 50, "Pass")
                            .when(col("mark") < 50, "Fail")
                            .otherwise("Absent"))

display(df1)

# COMMAND ----------

df2 = df.withColumn("status", when((col("mark") >= 80) & (col("Attendance") >= 80), "Distinction")
                             .when((col("mark") >= 50) & (col("Attendance") >= 50), "Good")
                             .otherwise("Average"))
df2.show()
