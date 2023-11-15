# Databricks notebook source
#DBFS , DB incluedes a variety of sample datasets mounted to
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# MAGIC %pyspark
# MAGIC df = spark.read.load('/data/products.csv',
# MAGIC     format='csv',
# MAGIC     header=True
# MAGIC )
# MAGIC display(df.limit(10))
