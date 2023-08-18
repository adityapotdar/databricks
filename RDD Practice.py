# Databricks notebook source
sc = spark.sparkContext
rdd_in = sc.textFile("dbfs:/FileStore/shared_uploads/potdaraditya99@gmail.com/input-1.txt")

# COMMAND ----------

rdd_data = rdd_in.filter(lambda x: not x.startswith("Property_ID"))
rdd_header = rdd_in.filter(lambda x: x.startswith("Property_ID"))

# COMMAND ----------

rdd2 = rdd_data.flatMap(lambda x: x.split(",")).map(lambda x: x.split("|"))

# COMMAND ----------

col_list = rdd_header.first().split("|")
col1 = col_list.index("Property_ID")
col2 = col_list.index("Location")
col3 = col_list.index("Size")
col4 = col_list.index("Prige_SQ_FT")

# COMMAND ----------

def num_price(d1, d2):
    res = float(d1) * float(d2)
    return str(res)

# COMMAND ----------

header_out = rdd_header.map(lambda x: x.split("|")[col1] + "|" + x.split("|")[col2] + "|Final Price")
data_out = rdd2.map(lambda x: x[col1] + "|" + x[col2] + "|" + num_price(x[col3], x[col4]))

final_rdd = header_out.union(data_out)

# COMMAND ----------

final_rdd.coalesce(1).saveAsTextFile("output.txt")