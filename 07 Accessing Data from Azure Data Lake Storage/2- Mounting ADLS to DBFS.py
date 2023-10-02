# Databricks notebook source
# MAGIC %md
# MAGIC ###Mounting ADLS to DBFS
# MAGIC This notebook goes over the details of mounting a cloud-based storage such as Azure Data Lake Storage onto Databricks File System. This would enable us to interact with a cloud object storage using familiar file paths relative to the DBFS. there are 3 steps before being able to use mounting ADLS to DBFS:
# MAGIC
# MAGIC **1- Register an Azure AD aplication to create a service principal**  
# MAGIC **2- Get the service principal 'contributer' access to ADLS**  
# MAGIC **3- Mount the ADLS to DBFS using the IDs and secret**
# MAGIC
# MAGIC
# MAGIC
# MAGIC Documentation Links:  
# MAGIC https://docs.databricks.com/en/dbfs/mounts.html  
# MAGIC https://docs.databricks.com/en/dbfs/mounts.html#mount-adls-gen2-or-blob-storage-with-abfs
# MAGIC
# MAGIC

# COMMAND ----------

# Setting up the configuration from the documentation link2: 
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

# Currently, we have nothing in our dbfs:/mnt/:
display(dbutils.fs.ls("/"))

# COMMAND ----------

# We need to create these values from Azure portal:
Application_ID = "e9b2a604-969f-4cd7-bf33-e5fce0b86dc6"

Tenant_ID = "fa1480f6-1b40-4106-9248-f244a5166337"

secret = "HG68Q~w5DG6_5OwYxWiUZZ5mljtMCrnLsRBACc.D"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "e9b2a604-969f-4cd7-bf33-e5fce0b86dc6",
          "fs.azure.account.oauth2.client.secret": "HG68Q~w5DG6_5OwYxWiUZZ5mljtMCrnLsRBACc.D",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/fa1480f6-1b40-4106-9248-f244a5166337/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@kiandatalake13.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

spark.read.csv("dbfs:/mnt/bronze/countries.csv", header=True).display()

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("PermissionTest").getOrCreate()

# Define the file path
file_path = "/mnt/bronze/countries.csv"

# Try to read the file
try:
    df = spark.read.csv(file_path, header=True)
    df.show()
except Exception as e:
    print("Error:", e)


# COMMAND ----------

