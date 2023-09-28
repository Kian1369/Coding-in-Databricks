# Databricks notebook source
"""
This short notebook goes through the concept of pivoting around a column which result in a matrix-shaped dataframe.
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

countries.display()

# COMMAND ----------

"""
For instance, if we group our dataframe by the 'sub_region_id' and then pivot around the 'region_id' column, we will 
get a matrix-shaped dataframe where for each sub_region_id row we have separate columns of region_id values:
"""

countries.groupBy('sub_region_id').pivot('region_id').sum('population').display()