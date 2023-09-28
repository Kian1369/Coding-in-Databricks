# Databricks notebook source
"""
We already saw that using .select() method would select one or more columns from our dataframe but what if 
we want to select one or more columns and remove them from our dataframe? 
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

#Selecting multiple columns using .select() method and save it in a variable named countries_2: 
countries_2 = countries.select('NAME', 'CAPITAL', 'POPULATION')
countries_2.display()

#Our initial dataframe (countries) still has those three columns:
countries.display()

# COMMAND ----------

#Using .drop() method off of our dataframe will remove the selected coloumn:
countries_3 = countries.drop(countries['ORGANIZATION_REGION_ID'])
countries_3.display()

# COMMAND ----------

"""
We can remove multiple coloums at once using .drop() method;
Notice this time, I will not be refrencing the dataframe inside the .drop() method (just another syntax).
"""

countries_4 = countries_3.drop('SUB_REGION_ID', 'TERMEDIATE_REGION_ID')
countries_4.display()

# COMMAND ----------

