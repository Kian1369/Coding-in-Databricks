# Databricks notebook source
"""
In spark.sql we can use filtering to filter specific rows in dataframes. Useful links: 
instr() function: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.instr.html#pyspark.sql.functions.instr
Logical Operations: https://spark.apache.org/docs/2.3.0/api/sql/index.html
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

#Filtering countries with population greater than 1e^9 (1 bilion):
countries.filter(countries['POPULATION'] > 1000000000).display()

# COMMAND ----------

"""
Locate() method:
It takes its first argument as our desired letter, and the second argument would be the column name and more importantly, outside locate() parentheses we need to specify a number to show where that letter would locate within the string:
"""

#First import locate from pyspark.sql.functions module
from pyspark.sql.functions import locate

#For instance, using locate() method to filter countries with their capital city starting with letter 'B':
countries.filter(locate("B", countries['CAPITAL'])==1).display()

# COMMAND ----------

#Combining some logical operation but note that each condition should be inside its own parentheses:
countries.filter( (locate("B", countries['CAPITAL'])==1) | (countries['POPULATION'] > 100000000) ).display()

# COMMAND ----------

#Using some sql syntax to filter specific rows (here, region_id equal to 10): 
countries.filter("REGION_ID == 10").display()

#Using some sql syntax to filter specific rows (here, region_id not equal to 10): 
countries.filter("REGION_ID != 10").display()

# COMMAND ----------

countries.filter("REGION_ID == 10 and POPULATION > 10000000").display()

# COMMAND ----------

"""
Filtering countries which their names are greater than 15 character and their region_id is not 10
1-Using sql syntax   2-Using another syntax, not sql
"""

from pyspark.sql.functions import length

countries.filter("length(name) > 15 and REGION_ID != 10").display()
countries.filter( (length(countries['NAME']) > 15) & (countries['REGION_ID'] != 10)).display()

# COMMAND ----------

