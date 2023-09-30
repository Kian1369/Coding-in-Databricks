# Databricks notebook source
"""
In order to get familiar with the "Medallion Architecture" please refer to the following link:
https://www.databricks.com/glossary/medallion-architecture

This notebook goes through a practical assignment regarding the previous sections materials + how 
to go from BRONZ medallion to SILVER and from SILVER to GOLD. We will be using 'Customers+Orders' 
datasets where we have 5 tables including: 1-ORDERS 2-ORDERS_ITEMS 3-CUSTOMERS 4-STORES and 5-PRODUCTS


***FIRST PART OF THE ASSIGNMNET***





"""



# COMMAND ----------


from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField

orders_FilePath = "dbfs:/FileStore/tables/bronz/orders.csv"
stores_FilePath = "dbfs:/FileStore/tables/bronz/stores.csv"

orders_Schema = StructType([StructField("ORDER_ID", IntegerType(), False),
                            StructField("ORDER_DATETIME", StringType(), False),
                            StructField("CUSTOMER_ID", IntegerType(), False),
                            StructField("ORDER_STATUS", StringType(), False),
                            StructField("STORE_ID", IntegerType(), False)
                            ]
                           )

stores_Schema = StructType([StructField("STORE_ID", IntegerType(), False),
                            StructField("STORE_NAME", StringType(), False),
                            StructField("WEB_ADDRESS", StringType(), False),
                            StructField("LATITUDE", DoubleType(), False),
                            StructField("LONGITUDE", DoubleType(), False)
                            ]
                           )


stores = spark.read.csv(path= stores_FilePath, schema=stores_Schema, header=True)
orders = spark.read.csv(path= orders_FilePath, schema=orders_Schema, header=True)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

ORDERS = orders.join(stores, orders['store_id']==stores['store_id'], 'left'). \
filter(orders['ORDER_STATUS']=='COMPLETE'). \
select(orders['ORDER_ID'], to_timestamp(orders['ORDER_DATETIME'], 'dd-MMM-yy HH.mm.ss.SS').alias('ORDER_TIMESTAMP'), orders['CUSTOMER_ID'], stores['STORE_NAME'])

ORDERS.display()
ORDERS.dtypes

# COMMAND ----------

ORDERS.write.parquet('dbfs:/FileStore/tables/silver/orders', mode='overwrite')

# COMMAND ----------

orders_item_filePath = 'dbfs:/FileStore/tables/bronz/order_items.csv'

orders_item_Schema = StructType([StructField("ORDER_ID", IntegerType(), False),
                                StructField("LINE_ITEM_ID", StringType(), False),
                                StructField("PRODUCT_ID", IntegerType(), False),
                                StructField("UNIT_PRICE", DoubleType(), False),
                                StructField("QUANTITY", IntegerType(), False)
                                ]
                                )

orders_item = spark.read.csv(path= orders_item_filePath, schema= orders_item_Schema, header=True)
orders_item.dtypes

ORDERS_ITEMS = orders_item.drop('LINE_ITEM_ID')
ORDERS_ITEMS.display()

# COMMAND ----------

ORDERS_ITEMS.write.parquet('dbfs:/FileStore/tables/silver/orders_item', mode='overwrite')

# COMMAND ----------

products_filePath = 'dbfs:/FileStore/tables/bronz/products.csv'

products_schema = StructType([StructField('PRODUCT_ID', IntegerType(), False),
                              StructField('PRODUCT_NAME', StringType(), False),
                              StructField('UNIT_PRICE', DoubleType(), False)
                            ]
                            )

PRODUCTS = spark.read.csv(path="dbfs:/FileStore/tables/bronz/products.csv", schema=products_schema, header=True)
PRODUCTS.display()
PRODUCTS.dtypes

# COMMAND ----------

PRODUCTS.write.parquet('dbfs:/FileStore/tables/silver/products', mode='overwrite')

# COMMAND ----------

customers_filePath = 'dbfs:/FileStore/tables/bronz/customers.csv'

customers_schema = StructType([StructField('CUSTOMER_ID', IntegerType(), False),
                              StructField('FULL_NAME', StringType(), False),
                              StructField('EMAIL_ADDRESS', StringType(), False)
                            ]
                            )

CUSTOMERS = spark.read.csv(path="dbfs:/FileStore/tables/bronz/customers.csv", schema=customers_schema, header=True)
CUSTOMERS.display()
CUSTOMERS.dtypes

# COMMAND ----------

CUSTOMERS.write.parquet('dbfs:/FileStore/tables/silver/customers', mode='overwrite')

# COMMAND ----------

"""
***SECOND PART OF THE ASSIGNMNET***

MOVING TABLES FROM SILVER STATUS TO GOLD


"""

# COMMAND ----------

#First of all, we should use 'to_date() method to change the 'ORDER_TIMESTAMP' column to 'day level' format 

from pyspark.sql.functions import to_date

Orders_details = ORDERS.select('ORDER_ID',to_date(ORDERS['ORDER_TIMESTAMP']).alias('DATE'), 'CUSTOMER_ID', 'STORE_NAME')
Orders_details.display()

# COMMAND ----------

#Then, we left-join 'Orders_details' tab;e with 'ORDERS_ITEMS' on "ORDER_ID" column since we want all the records from the left table.

joined_tables = Orders_details.join(ORDERS_ITEMS, ORDERS['ORDER_ID']==ORDERS_ITEMS['ORDER_ID'], 'left'). \
select(Orders_details['ORDER_ID'], Orders_details['DATE'], Orders_details['CUSTOMER_ID'], Orders_details['STORE_NAME'], 
      ORDERS_ITEMS['UNIT_PRICE'], ORDERS_ITEMS['QUANTITY'])

joined_tables.display()

# COMMAND ----------

#At this step, we create another column named 'TOTAL_SALE_AMOUNT' which is the product of each row's 'UNIT_PRICE' and 'QUANTITY'
joined_tables = joined_tables.withColumn('TOTAL_SALE_AMOUNT', ORDERS_ITEMS['UNIT_PRICE']*ORDERS_ITEMS['QUANTITY'])
joined_tables.display()

# COMMAND ----------

#Lastly, grouping by 'ORDER_ID', 'DATE', 'CUSTOMER_ID', and 'STORE_NAME' columns and taking the sum of 'TOTAL_SALE_AMOUNT'
#and assigning the result back to the order_details dataframe:

from pyspark.sql.functions import round, asc, desc

order_details = joined_tables.groupBy('ORDER_ID', 'DATE', 'CUSTOMER_ID', 'STORE_NAME').sum('TOTAL_SALE_AMOUNT')
order_details.display()

#We also need to rename the 'sum(TOTAL_SALE_AMOUNT)' to 'TOTAL_SALE_AMOUNT' and round the values to 2 decimal place, and finally, 
#sort the result in descending order:

order_details = order_details.withColumnRenamed('sum(TOTAL_SALE_AMOUNT)', 'TOTAL_SALE_AMOUNT')
order_details = order_details.withColumn('TOTAL_SALE_AMOUNT', round('TOTAL_SALE_AMOUNT', 2)).sort(desc('TOTAL_SALE_AMOUNT'))
order_details.display()

#Writing our final table to the desired folder and path in .parquet format
order_details.write.parquet('dbfs:/FileStore/tables/gold/order_details', mode='overwrite')

# COMMAND ----------

"""
For the second table we need to use 'date_format()' method to sort the dates in yyyy-MM format and then we would be
able to aggregate for 'TOTAL_SALE_AMOUNT' based on each month and year.

Useful Links: 
https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions
"""

from pyspark.sql.functions import date_format

monthly_sale = order_details.withColumn('MONTHLY_YEAR', date_format('DATE', 'yyyy-MM'))
monthly_sale.display()


# COMMAND ----------

monthly_sale = monthly_sale.groupBy('MONTHLY_YEAR').sum('TOTAL_SALE_AMOUNT'). \
withColumn('TOTAL_MONTHLY_SALE', round('sum(TOTAL_SALE_AMOUNT)', 2)). \
sort(asc('MONTHLY_YEAR'))



# COMMAND ----------

monthly_sale = monthly_sale.drop('sum(TOTAL_SALE_AMOUNT)')
monthly_sale.display()


#Writing our final table to the desired folder and path in .parquet format
monthly_sale.write.parquet('dbfs:/FileStore/tables/gold/monthly_sale', mode='overwrite')


# COMMAND ----------

#Now we want to show the monthly sale by the each specific store:
sales_with_month = order_details.withColumn('MONTH_YEAR', date_format('DATE', 'yyyy-MM'))

sales_with_month.display()

# COMMAND ----------

store_monthly_sale = sales_with_month.groupBy('STORE_NAME', 'MONTH_YEAR').sum('TOTAL_SALE_AMOUNT'). \
withColumn('TOTAL_SALES', round('sum(TOTAL_SALE_AMOUNT)', 2)).sort(asc('STORE_NAME')). \
select('STORE_NAME', 'MONTH_YEAR', 'TOTAL_SALES')

store_monthly_sale.display()

#Writing our final table to the desired folder and path in .parquet format
store_monthly_sale.write.parquet('dbfs:/FileStore/tables/gold/store_monthly_sale', mode='overwrite')


# COMMAND ----------

