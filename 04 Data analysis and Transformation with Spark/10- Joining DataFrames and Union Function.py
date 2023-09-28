# Databricks notebook source
"""
Being familiar with inner join, left-join, right-join, and outer-join is highly recommended.
Useful Links for reading about different types of join: 
https://www.w3schools.com/sql/sql_join.asp
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join
"""

#Storing our csv file in a variable called countries_path:
countries_path = "dbfs:/FileStore/tables/countries.csv"

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
countries = spark.read.csv(path=countries_path, header=True, schema=countries_schema) 

# COMMAND ----------

#Storing our other csv file in a variable called regions_path:
regions_path = 'dbfs:/FileStore/tables/country_regions.csv'

#Storing schema into a variable:
regions_schema = StructType([
                    StructField("ID", StringType(), False),
                    StructField("NAME", StringType(), False),
                    ]
                    )

#Reading the file into a dataframe called regions:
regions = spark.read.csv(path=regions_path, header=True, schema=regions_schema) 

# COMMAND ----------

"""
By displaying both dataframes we notice that the 'region_id' from countries dataframe corresponds with
the 'ID' column from regions dataframe, so we can join these two dataframes by mentioned columns:
"""

countries.display()
regions.display()

# COMMAND ----------

#Inner join based on 'REGION_ID' and 'ID' columns:
countries.join(regions, countries['REGION_ID']==regions['ID'], 'inner').display()


# COMMAND ----------

#We can use .select() method following our join method to select our desired columns only:
countries.join(regions, countries['REGION_ID']==regions['ID'], 'inner'). \
select(countries['NAME'], countries['CAPITAL'], countries['POPULATION'], countries['REGION_ID'], regions['NAME']). \
display()

# COMMAND ----------

"""
Performing an inner-join on "countries" and "regions" dataframes, displaying only the region name 
column and countries name and population columns. Then aliasing the region name as region_name and 
country name as country_name, and finally, sorting the result in descending population order:
"""

countries.join(regions, countries['REGION_ID']==regions['ID'], 'inner'). \
select(countries['name'].alias('country_name'), countries['population'], regions['name'].alias('region_name')). \
sort(countries['population'].desc()).display()

# COMMAND ----------

#We can use .union() method to union a dataframe with another dataframe. Note that a conditon we need
#to pay attention to is that both dataframes must have the same number of columns:
countries.union(countries).display()

#When we use .count() method we notice that the number of records (a.k.a rows) would be doubled:
countries.union(countries).count()