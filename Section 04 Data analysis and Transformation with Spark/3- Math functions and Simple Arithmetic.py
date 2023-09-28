# Databricks notebook source
"""
Please refer to "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#math-functions" link
for more mathematic and arithmetic functions references.
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

#We saw in previous sections that we can convert the population into million like below:
countries_2 = countries.withColumn('POPULATION', countries['POPULATION']/1000000)
countries_2 = countries_2.withColumnRenamed('POPULATION', 'POPULATION_MILLION')
countries_2.display()



# COMMAND ----------

"""
To round a column with integer data type we should import 'round' from pyspark.sql.functions, and then use the following syntax:
(Note that this approach will create and add another column named 'POPULATION_MILLION_ROUND' to the end of the dataframe)
"""
from pyspark.sql.functions import round

countries_2.withColumn('POPULATION_MILLION_ROUND', round('POPULATION_MILLION', 3)).display()

# COMMAND ----------

#In order to round the 'POPULATION_MILLION' column and replace it (not adding a new column) we can do:
countries_2.select(round(countries_2['POPULATION_MILLION'], 3)).display()

# COMMAND ----------

