# Databricks notebook source
"""
Useful links: 
Aggregate functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions
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

"""
Using groupBy() function we can group our dataframe by a column but this will result in creating a "GroupedData" object.
Therefore, we can not use .display() off of a GroupedData object.
"""

countries.groupBy('region_id')

# COMMAND ----------

#Adding .sum() method would return a DataFrame[region_id: int, sum(population): double] object, so we can use .display():
countries.groupBy('region_id').sum('population').display()

#We can use more than one column inside .sum() method: 
countries.groupBy('region_id').sum('population', 'AREA_KM2').display()

# COMMAND ----------

#Grouping by 'region_id' then using .min() or .avg() method:
countries.groupBy('region_id').min('population').display()
countries.groupBy('region_id').avg('population').display()

# COMMAND ----------

"""
If we want to use more than one aggregation functions we need to use .agg() method first, and then inside 
the .agg() method we will use our desired aggregate functions such as sum(), avg(), etc.
"""

from pyspark.sql.functions import *

countries.groupBy('region_id').agg(avg('population'), sum('AREA_KM2')).display()

# COMMAND ----------

#Similarly, we can group by multiple columns as well:
countries.groupBy('region_id', 'sub_region_id').agg(avg('population'), sum('AREA_KM2')).display()

# COMMAND ----------

"""
This time I will use the same code as above but rename the "avg(population)" and "sum(AREA_KM2)" columns
as well as using another syntax for clearer readability of our code when it gets long:
"""

countries.groupBy('REGION_ID', 'SUB_REGION_ID').agg(avg('population'), sum('AREA_KM2')). \
withColumnRenamed('avg(population)', 'Average_Population'). \
withColumnRenamed('sum(AREA_KM2)', 'Total_Area_Km2'). \
display()


#Another approach for renaming the columns would be using .alias() method:
countries.groupBy('REGION_ID', 'SUB_REGION_ID'). \
agg(avg('population').alias('Average_Population'), sum('AREA_KM2').alias('Total_Area_Km2')). \
display()


# COMMAND ----------

"""
Grouping by 'region_id', 'sub_region_id' columns, then showing the maximum and minimum population of each 
region and sub-region as 'max_pop' and 'min_pop', and lastly, sorting the 'region_id' column in ascending order
"""

countries.groupBy('region_id', 'sub_region_id'). \
agg(max('population').alias('max_pop'), min('population').alias('min_pop')). \
sort(countries['region_id'].asc()). \
display()

# COMMAND ----------

