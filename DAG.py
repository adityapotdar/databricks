# Databricks notebook source
departments = {0: 'Technology', 1: "HR", 2: "Devops", 3: 'Sales', 4: 'Marketing', 5: 'Business'}
test = list(departments.items())
print(test)

# COMMAND ----------

import random

empDetails = dict()
for i in range (10000):
    empDetails[i] = i, random.randint(0, 5)

empList = list(empDetails.values())

# COMMAND ----------

deptDf = spark.createDataFrame(test, ["id", "name"])
empDf = spark.createDataFrame(empList, ["id", "dept_id"])

display(deptDf)
display(empDf)

# COMMAND ----------

joinDf = empDf.join(deptDf, empDf.dept_id == deptDf.id, "inner")
joinDf.count()

# COMMAND ----------

