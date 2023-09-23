# Databricks notebook source
df = spark.read.csv("dbfs:/FileStore/tables/countries.csv", inferSchema=True, header=True)
display(df)

# COMMAND ----------

#Writing our dataframe as a parquet file: 
#(Note that parquet files have their coloumn names and data types in them, so no need to specify them)
df.write.parquet("dbfs:/FileStore/tables/output/countries_parquet")

# COMMAND ----------

#Reading the parquet file:
display(spark.read.parquet("dbfs:/FileStore/tables/output/countries_parquet"))