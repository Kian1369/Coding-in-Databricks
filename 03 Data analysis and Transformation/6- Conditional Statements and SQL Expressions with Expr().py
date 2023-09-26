# Databricks notebook source
"""
This notbook goes through some concepts about 'conditional statements' using different syntaxs
as well as using exact SQL expressions with expr() function.
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

#Using 'When' statement to say if the population is greater than 100M returns "Large", otherwise "Not Large"

from pyspark.sql.functions import when

countries.withColumn('country_name_length', when(countries['POPULATION'] > 100000000, "Large").when(countries['POPULATION'] <= 100000000, "Not Large")).display()

# COMMAND ----------

"""
Another syntax using 'when' and 'otherwise':
Note that we can use as many 'when' statements as we want, and add an otherwise statement at the end.
"""

countries.withColumn('country_name_length', when(countries['POPULATION'] > 100000000, "Large").otherwise("Not Large")).display()

# COMMAND ----------

"""
We can use exact SQL syntax using expr() function;
We need to import expr() function from 
"""

from pyspark.sql.functions import expr

#So instead of using alias() method from spark API we can do the follwing:
countries.select(expr("NAME AS COUNTRY_NAME")).display()

# COMMAND ----------

#Another Example:
countries.select(expr("left(NAME,3) AS Shortened_Name")).display()

# COMMAND ----------

#Creating a new column named 'POPULATION_ClASS' and combine it with SQL CASE - END statements:
countries.withColumn('POPULATION_ClASS', expr("CASE WHEN population > 100000000 THEN 'Large' WHEN population > 50000000 THEN        'Medium' ELSE 'Small' END")).display()

# COMMAND ----------

#Creating a new column named 'AREA(KM^2)_ClASS' and combine it with SQL CASE - END statements:
countries.withColumn('AREA(KM^2)_ClASS', expr("CASE WHEN AREA_KM2 > 1000000 THEN 'Large' WHEN AREA_KM2 > 300000 THEN 'Medium' ELSE 'Small' END")).display()