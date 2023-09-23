# Databricks notebook source
#Reading our dataframe in using "spark.read.csv()" and storing it in countries_df variable
countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

#displaying countries_df dataframe with .show() method
countries_df.show()

# COMMAND ----------

#display() method would show the content of the dataframe nicer that .show()
#Also adding the header=True option would show the first row as header
countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv", header=True)
display(countries_df)

# COMMAND ----------

#Another way to read the data with .option approach: 
countries_df = spark.read.options(header=True).csv("dbfs:/FileStore/tables/countries.csv")
display(countries_df)

# COMMAND ----------

#Three different ways to get the columns' data types:

# 1-Using .dtypes attribute:
countries_df.dtypes 


# COMMAND ----------

# 2-Using .schema attribute: 
#(Note that the third parameter in StructField() is a boolean which shows whether that data type could be nullable or not)
countries_df.schema

# COMMAND ----------

# 3- Using .describe() method:
countries_df.describe()

# COMMAND ----------

# "StructType" is a class that represents the schema of a DataFrame. It is essentially a collection of StructField objects that define the individual columns of the DataFrame along with their names and data types and a boolean argument. You can think of StructType as the overall blueprint that defines the structure of the data, while the "StructField" is a class that represents an individual column within a schema 
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

#Now when we pass "countries_schema" as a parameter to .schema() method, the coloums' data types are in their correct format
countries_df = spark.read.options(header=True).schema(countries_schema).csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

#Reading single-line json file:
countries_SL_json = spark.read.json("dbfs:/FileStore/tables/countries_single_line.json")

# COMMAND ----------

display(countries_SL_json)

# COMMAND ----------

#Reading multiple-line json file:
#(Note that for reading multiple-line json files we need to .options() method and set "multiLine=True")
countries_ML_json = spark.read.options(multiLine=True).json("dbfs:/FileStore/tables/countries_multi_line.json")

# COMMAND ----------

display(countries_ML_json)

# COMMAND ----------

#Two ways for reading .txt files we can use spark.read.csv() with the following arguments:
countries_txt = spark.read.csv("dbfs:/FileStore/tables/countries.txt", header=True, sep="\t")
countries_txt = spark.read.options(header=True, sep="\t").csv("dbfs:/FileStore/tables/countries.txt")

# COMMAND ----------

display(countries_txt)