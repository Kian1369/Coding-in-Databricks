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


# COMMAND ----------

countries.display()

# COMMAND ----------

#To select specific columns we use .select() method off of the dataframe by passing the columns' name as arguments:
#(Note: We can use both uppercase and lower case for columns' name)
countries.select("name", "capital", "POPULATION").display()

# COMMAND ----------

#Another approach would be as bellow: (Note: We can use both uppercase and lower case for columns' name)
countries.select(countries['name'], countries['capital'], countries['population']).display()

# COMMAND ----------

#Another way to select columns' name would be like this: (Note: For this method we have use uppercase only, unless won't work)
countries.select(countries.NAME, countries.CAPITAL, countries.POPULATION).display()


# COMMAND ----------

#Lastly, we can import 'col' from pyspark.sql.functions and do the same task:
from pyspark.sql.functions import col

countries.select(col('name'), col('capital'), col('population')).display()

# COMMAND ----------

#Renaming the columns using .alies() method: 
countries.select(countries['name'].alias('Country_Name'), 
                 countries['capital'].alias('Capital_City'), countries['Population']).display()

# COMMAND ----------

#Renaming the columns using .withColumnRenamed() method: 
countries.select('name', 'capital', 'population').withColumnRenamed('name','Country_Name').withColumnRenamed('capital',        'Capital_City').display()

# COMMAND ----------

#To Add a new column to our dataframe we can use .withColumn() method, passing the new column's name then the data
#Here in this example, I am importing 'current_date' from pyspark.sql.functions to fill in the new column with current date data:
from pyspark.sql.functions import current_date

addedColumnCountries = countries.withColumn('Current Date', current_date())
addedColumnCountries.display()

# COMMAND ----------

# importing 'lit' (meaning literal) from pyspark.sql.functions to fill in the new added column with literal values:
from pyspark.sql.functions import lit

addedColumnCountries.withColumn('Updated_By', lit('Kian')).display()

# COMMAND ----------

#Using .withColumn to replace 'POPULATION' column but this time showing the countries' population in million:
addedColumnCountries.withColumn('POPULATION', addedColumnCountries['population']/1000000).display()

# COMMAND ----------

