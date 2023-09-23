# Databricks notebook source
# First, let's copy and paste the schema we created from the previous part (Reading Data)
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

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

# COMMAND ----------

#Creating a dataframe from file store and the schema we created above:
countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv", header=True, schema=countries_schema)

#Writing the created dataframe to desired path + adding the desired name to the end of it
countries_df.write.csv("dbfs:/FileStore/tables/countries_output", header=True)

# COMMAND ----------

#To overwrite the existing file simply add the mode='overwrite' parameter:
countries_df.write.csv("dbfs:/FileStore/tables/countries_output", header=True, mode='overwrite')

#Or alternatively: 
countries_df.write.mode('overwrite').csv("dbfs:/FileStore/tables/countries_output", header=True)

# COMMAND ----------

#Now let's make our dataframe partitioned (basically make smaller datasets) by column "REGION_ID"
countries_df.write.options(header=True).mode("overwrite").partitionBy("REGION_ID").csv("dbfs:/FileStore/tables/countries_output")

# COMMAND ----------

#Now we can easily display one of our smaller datasets (here REGION_ID=10)
firstPartioned = spark.read.csv("dbfs:/FileStore/tables/countries_output/REGION_ID=10", header=True)
display(firstPartioned)

# COMMAND ----------

