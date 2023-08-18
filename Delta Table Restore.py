# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY scd2demo

# COMMAND ----------

from delta.tables import *

targetTable = DeltaTable.forName(spark, "scd2demo")
display(targetTable.history())

# COMMAND ----------

targetTable.restoreToVersion(3)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM scd2demo

# COMMAND ----------

targetTable.restoreToTimestamp("2023-07-28T04:43:35.000+0000")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM scd2demo

# COMMAND ----------

display(targetTable.history())