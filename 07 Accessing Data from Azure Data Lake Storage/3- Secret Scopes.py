# Databricks notebook source
# MAGIC %md
# MAGIC ###Secret Scopes
# MAGIC In order to make sure that our 'application id', 'tenant id', and secret values are not displayed in our notebooks, we can use dbutils.secrets.get() method.

# COMMAND ----------

# We need to create these values from Azure portal:
application_id = "e9b2a604-969f-4cd7-bf33-e5fce0b86dc6"

tenant_id = "fa1480f6-1b40-4106-9248-f244a5166337"

secret = "lez8Q~sc8eKMLXTZBRiMBb7g14QMB1GXXKtTnbXj"

# COMMAND ----------

container_name = "bronze"
account_name = "kiandatalake13"
mount_point = "/mnt/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC Before going through secret scopes concept, notice that another way for passing the values would be using python f string as follows:

# COMMAND ----------

configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": application_id,
        "fs.azure.account.oauth2.client.secret": secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        }
        
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Azure Key Vaults
# MAGIC
# MAGIC Key vaults is a cloud service that provides a secure store for our secrets. We can securely store our keys, passwords, certificates as well as other secrets. So we need to search for **"key vaults"** in our Azure portal and create one. After creating we need to click on **'secrets'** from objects title on the lest side of the screen, and finally click on **'Generate/Import'.** We need to generate three different values for our application_id, tenant_id, and secret.
# MAGIC
# MAGIC Then we need to go to our databricks URL instance (mine is: https://adb-846360804422055.15.azuredatabricks.net/?o=846360804422055#)
# MAGIC and add /secrets/createScope at the end of the link (after #). So the final link for me would be like: 
# MAGIC
# MAGIC https://adb-846360804422055.15.azuredatabricks.net/?o=846360804422055#/secrets/createScope
# MAGIC
# MAGIC Finally, we need to insert values for **scope name**, **DNS name**, and **resource id**. We can find these values from our key vault secret that we just created, below the setting option click on **'properties'** and you see the values for **DNS name**, and **resource id:**
# MAGIC

# COMMAND ----------

#Accessing our secrets would be as follows: 
application_id = dbutils.secrets.get(scope="databricks-secrets-kian", key="application-id")
tenant_id = dbutils.secrets.get(scope="databricks-secrets-kian", key="tenant-id")
secret = dbutils.secrets.get(scope="databricks-secrets-kian", key="secret-value")

# COMMAND ----------

configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": application_id,
        "fs.azure.account.oauth2.client.secret": secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        }
        
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

