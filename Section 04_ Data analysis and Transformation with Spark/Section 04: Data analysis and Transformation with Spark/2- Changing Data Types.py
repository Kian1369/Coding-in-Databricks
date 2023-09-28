# Databricks notebook source
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

countries.dtypes

# COMMAND ----------

#Let's read in our csv file again but this time without specifying the schema:
countries_dt = spark.read.csv(path=path, header=True)

#Now when we check the data types of our dataframe we notice that all of them are 'String' data type
countries_dt.dtypes

# COMMAND ----------

#We can change any columns' data type using cast function from pyspark.sql.types as follows:
countries_dt.select(countries_dt['POPULATION'].cast(IntegerType())).dtypes

# COMMAND ----------

#Note that the changes we made would not be saved unless we use 
#.withColumn() method and assign these changes to a new variable:
from pyspark.sql.functions import col

countries_dt = countries_dt.withColumn("POPULATION", col("POPULATION").cast(IntegerType()))
countries_dt.dtypes
countries_dt.display()

# COMMAND ----------

