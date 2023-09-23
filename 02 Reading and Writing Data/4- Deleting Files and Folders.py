# Databricks notebook source
#dbutils is a Databricks utility available in Databricks notebooks, and fs stands for filesystem
#Using .help() method off of dbutils.fs gives us useful information about "fsutils" and "mount"
dbutils.fs.help() 

# COMMAND ----------

#In order to delete files (.txt .json files here):
dbutils.fs.rm("dbfs:/FileStore/tables/countries.txt")
dbutils.fs.rm("dbfs:/FileStore/tables/countries_multi_line.json")
dbutils.fs.rm("dbfs:/FileStore/tables/countries_single_line.json")

# COMMAND ----------

#In order to delete directories that contain files:
dbutils.fs.rm("dbfs:/FileStore/tables/output", recurse=True)