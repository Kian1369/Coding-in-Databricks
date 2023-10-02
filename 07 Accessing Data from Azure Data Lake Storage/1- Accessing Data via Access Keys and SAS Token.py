# Databricks notebook source
# MAGIC %md
# MAGIC ###Accessing Accessing Data via Access Keys:
# MAGIC
# MAGIC In order to access your data via access key you just need to go to your storage account first and then click on "Access Keys" right below Security + networking option and copy one of the key1 or key2 options.
# MAGIC
# MAGIC Link for documentation: https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage

# COMMAND ----------

#My Key1:
oPBT6pUSfzXT9ozFeag0QXRA1WLhqAaVcNyKR5ijcgb2QpU8gzfSX0RsWInrjov3LX7C7mH8kUpM+ASth1ejBQ==

# COMMAND ----------

#Setting up the configuration (from the documentation): 
spark.conf.set(
    "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

#Inserting my key and my storage account name into the above code:
spark.conf.set(
    "fs.azure.account.key.datalake1369.dfs.core.windows.net",
    "oPBT6pUSfzXT9ozFeag0QXRA1WLhqAaVcNyKR5ijcgb2QpU8gzfSX0RsWInrjov3LX7C7mH8kUpM+ASth1ejBQ==")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that for the file path we need to use a **URI** which stands for **uniform resource identifier**
# MAGIC
# MAGIC Documentation Link: https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver

# COMMAND ----------

"""
You can access this code from the documentation mentioned above. Simply replace the file_system with your container name
and acoount_name with your account name, and copy and paste your file path at the end:
"""
abfss://file_system@account_name.dfs.core.windows.net/<path>/<path>/<file_name>

# COMMAND ----------

#So this would be my URI:
abfss://bronze@datalake1369.dfs.core.windows.net/countries.csv


# COMMAND ----------

#Reading the countries_csv file from our bronze container in our ADLS Gen2 storage account:
countries = spark.read.csv("abfss://bronze@datalake1369.dfs.core.windows.net/countries.csv", header=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Accessing Accessing Data via SAS Tokens:
# MAGIC
# MAGIC In order to access your data via SAS Tokens (stands for Shared Access Signiture) you just need to set up the configuration first as below, and then replace storage_account with our storage account name and also replce the SAS token:
# MAGIC
# MAGIC Link for documentation: https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage

# COMMAND ----------

#Setting up the configuration via SAS Token (ode from the documentation):
spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

# COMMAND ----------

# MAGIC %md
# MAGIC We need to go to our storage account first and then click on "shared access signature" right below Security + networking option and check every optiones below "Allowed resource types". Then we need to set a "Start and expiry date/time". Finally generate the SAS Token.

# COMMAND ----------

#If we get a ? mark at the begging of our SAS token string we need to delet it:
spark.conf.set("fs.azure.account.auth.type.datalake1369.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalake1369.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalake1369.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-08T02:00:46Z&st=2023-09-07T18:00:46Z&spr=https&sig=izzrG0%2BSnPVb0UAqBEcWJzT%2BlsiLpiVI%2BpmsBmo%2FjTs%3D")

# COMMAND ----------

#Our URI would be the same as before:
abfss://bronze@datalake1369.dfs.core.windows.net/countries.csv

# COMMAND ----------

#Reading the countries_csv file from our bronze container in our ADLS Gen2 storage account via SAS Token:
spark.read.csv("abfss://bronze@datalake1369.dfs.core.windows.net/countries.csv", header=True).display()