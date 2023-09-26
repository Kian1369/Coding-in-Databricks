# Databricks notebook source
"""
Please refer to these links for more information about'Sorting', 'String', 'DateTime' functions:
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#sort-functions 
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#string-functions 
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions

"""

#Storing our csv file in a variable called path:
path = "dbfs:/FileStore/tables/countries.csv"


from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
#Storing schema into a variable:
countries_schema = StructType([
                    StructField("COUNTRY_ID", IntegerType(), False),
                    StructField("NAME", StringType(), False),
                    StructField("NATIONALITY", StringType(), False),
                    StructField("COUNTRY_CODE", IntegerType(), False),
                    StructField("ISO_ALPHA2", StringType(), False),
                    StructField("CAPITAL", StringType(), False),
                    StructField("POPULATION", DoubleType(), False),
                    StructField("AREA_KM2", IntegerType(), False),
                    StructField("REGION_ID", IntegerType(), True),
                    StructField("SUB_REGION_ID", IntegerType(), True),
                    StructField("TERMEDIATE_REGION_ID", IntegerType(), True),
                    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
                    ]
                    )

#Reading the file into a dataframe called countries:
countries = spark.read.csv(path=path, header=True, schema=countries_schema) 

# COMMAND ----------

#Importing required functions from 'pyspark.sql.functions' module
from pyspark.sql.functions import asc,desc 

#Using sort() method off of the countries dataframe, and inside the method we call .asc() or .desc() method:
countries.sort(countries['POPULATION'].asc()).display()
countries.sort(countries['POPULATION'].desc()).display()

# COMMAND ----------

"""
Now let's look at some functions for working with 'Strings';
First, import all the functions from pyspark.sql.functions module:
"""
from pyspark.sql.functions import *

#The upper() and lower() methods would show the strings in uppercase and lowercase respectively:
countries.select(upper(countries['NAME'])).display()

#For capitalizing the initial character of a string we can use initcap(): 
countries.select(initcap(countries['NAME'])).display()

#To get the length of each string we can use length() method:
countries.select(length(countries['NAME'])).display()



# COMMAND ----------

"""
Now what if we want to have a cloumn with country names + their unique country codes in front, separated with a comma and a whitespace?
We could use concat_ws() method; The first argument we pass would be a character(s) we want to use as a separator, and the rest arguments would be the columns we want to use this method off of:
"""

countries.select(concat_ws(', ', countries['NAME'], countries['CAPITAL'])).display()

# COMMAND ----------

#Note that we can combine 'String' functions. For instance: 
countries.select(concat_ws(', ', upper(countries['NAME']), countries['CAPITAL'])).display()

# COMMAND ----------

#Adding a 'timestamp' column to the end of the dataFrame, filling in with current timestamp:
countries_2 = countries.withColumn('timestamp', current_timestamp())
countries_2.display()

# COMMAND ----------

"""
Extracting the month from the 'timestamp' column:
(We can similarly use day() and year() methods to extract the desired results)
"""
countries_2.select(month(countries_2['timestamp'])).display()

# COMMAND ----------

