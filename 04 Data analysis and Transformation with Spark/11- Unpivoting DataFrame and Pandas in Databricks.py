# Databricks notebook source
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

#Joining our dataframes and selecting only 'country_name', 'region_name' and 'POPULATION' columns:
countries_join = countries.join(regions, countries['REGION_ID']==regions['ID'], 'inner'). \
select(countries['NAME'].alias('country_name'), regions['NAME'].alias('region_name'), countries['POPULATION'])

countries_join.display()

# COMMAND ----------

#Let's pivote the above dataframe by the 'region_name'
countries_pivot = countries_join.groupBy('country_name').pivot('region_name').sum('population')
countries_pivot.display()

# COMMAND ----------

"""
Now if we want to unpivot our dataframe and put the continents all in one column named 'region_name' 
and store their values as a separate column named 'population', we need to use the "stack" function 
inside the 'select' method, using the SQL syntax

stack takes the number of column values as the first argument (here = 5 because we have 5 columns for continents)
and then a sequence of literal names (i.e. 'Africa', 'America' , ...) and then the name of the column to take the 
value from (Here columns would be: Africa, America, Asia, Europe, and Oceania) and finally, specify the name of the
new columns ('region_name' and ''population)

Pay attention to the syntax since it might seem a little weird and tricky:
"""

from pyspark.sql.functions import expr

#Since we are using SQL syntax there is no need to import stack function from pyspark.sql.functions
unpivot_countries = countries_pivot.select('country_name', expr("stack(5, 'Africa', Africa, 'America', America, 'Asia', Asia, 'Europe', Europe, 'Oceania', Oceania) as (region_name, population)"))

unpivot_countries.display()


# COMMAND ----------

#To remove the null value from the 'population' column"
unpivot_countries_noNull = unpivot_countries.filter('population is not null').display()

# COMMAND ----------

"""
So far our dataframes that are stored in their variables have been "spark" dataframe. We can import 
pandas and use toPandas() method to convert our spark dataframes to Pandas dataframes easily:
"""

import pandas as pd

countries_pd = countries.toPandas()
countries_pd.head()

# COMMAND ----------

#Using iloc[] method from Pandas:
countries_pd.iloc[13]