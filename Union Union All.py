# Databricks notebook source
employee_data = [(100, "Stephen", "1999", "100", "M", 2000),
(200, "Philipp", "2002","200", "M", 8000),
(300, "John", "2010","100","", 6000),
]
employee_schema = ["employee_id", "name", "doj", "employee_dept_id", "gender", "salary"]
DF1 spark.createDataFrame(data=employee_data, schema= employee_schema)
display (DF1)