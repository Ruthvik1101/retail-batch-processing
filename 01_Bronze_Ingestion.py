# Databricks notebook source
# DBTITLE 1,Orders
df_orders = (spark.read.option("header", True)
             .option("inferSchema", True)
             .csv("/Volumes/jobs/bronze/data/orders.csv"))

display(df_orders.limit(10))

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

print("Total Records: ", df_orders.count())

# COMMAND ----------

df_orders.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("jobs.bronze.bronze_orders")


# COMMAND ----------

bronze_orders = spark.table("jobs.bronze.bronze_orders")
display(bronze_orders.limit(10))

# COMMAND ----------

display("Total_records: ", bronze_orders.count())

# COMMAND ----------

# DBTITLE 1,Customers
df_customers = (spark.read.option("header", True)
             .option("inferSchema", True)
             .csv("/Volumes/jobs/bronze/data/customers.csv"))

display(df_customers.limit(10))

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

display("Total_Records: ", df_customers.count())

# COMMAND ----------

df_customers.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("jobs.bronze.bronze_customers")



# COMMAND ----------

bronze_customers = spark.table("jobs.bronze.bronze_customers")
display(bronze_customers.limit(10))

# COMMAND ----------

display("Total_Records: ", bronze_customers.count())

# COMMAND ----------

# DBTITLE 1,Products
df_products = (spark.read.option("header", True)
             .option("inferSchema", True)
             .csv("/Volumes/jobs/bronze/data/products.csv"))

display(df_products.limit(10))

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

display(df_products.count())

# COMMAND ----------

df_products.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("jobs.bronze.bronze_products")

# COMMAND ----------

bronze_products = spark.table("jobs.bronze.bronze_products")
display(bronze_products.limit(10))


# COMMAND ----------

display(bronze_products.count())