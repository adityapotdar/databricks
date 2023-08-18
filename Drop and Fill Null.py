# Databricks notebook source
data_student = [
    ("Michael", "Science", 80, "P",90),
    ("Nancy", "Mathematics", 90, "P", None),
    ("David", "English", 20, "F",80),
    ("John", "Science", None, "F", None),
    ("Martin", "Mathematics", None, None, 70),
    (None, None, None, None, None)]

Schema= ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame(data = data_student, schema= Schema)
display(df)

# COMMAND ----------

#display(df.na.drop()) #any by default, drop row if any column contains null values
display(df.na.drop("all")) #drop row that contains all null values

# COMMAND ----------

display(df.na.drop(subset=["mark", "attendance"]))

# COMMAND ----------

display(df.na.fill(value = 0))

# COMMAND ----------

display(df.na.fill(value = "NA"))

# COMMAND ----------

display(df.na.fill({"mark": 0, "status": "NA", "attendance": "NA"}))

# COMMAND ----------

