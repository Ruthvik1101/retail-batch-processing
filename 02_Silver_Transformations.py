# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_customer = spark.table("jobs.bronze.bronze_customers")
df_orders = spark.table("jobs.bronze.bronze_orders")
df_product = spark.table("jobs.bronze.bronze_products")

print("Customers: ", df_customer.count())
print("Orders: ", df_orders.count())
print("Products: ", df_product.count())

# COMMAND ----------

silver_customers = (
    df_customer
    .withColumn(
        "signup_date", F.to_date("signup_date")
    )
    
    .withColumn(
        "email", F.lower(F.trim(F.col("email")))
    )

    .filter(
        F.col("customer_id").isNotNull()
        & F.col("email").contains("@")
        &F.col("signup_date").isNotNull()
    )

    .dropDuplicates(["customer_id"])
    .select("customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "signup_date",
        "customer_segment"
    )

)

display(silver_customers)

# COMMAND ----------

silver_products = (
    df_product
    .withColumn(
        "unit_price", F.col("unit_price").cast("decimal(12,2)")
    )
    .withColumn(
        "cost_price", F.col("cost_price").cast("decimal(12, 2)")
    )
    .filter(
        F.col("product_id").isNotNull() 
        & (F.col("unit_price") > 0)
        & (F.col("cost_price") >= 0)
    )
    .dropDuplicates(["product_id"])
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "cost_price",
        "is_active"
    )
)

display(silver_products)

# COMMAND ----------

valid_statuses = [
    "Delivered",
    "Processing",
    "Cancelled",
    "Returned"
]

silver_orders = (
    df_orders
    .withColumn(
        "order_date",
        F.to_date("order_date")
    )
    .withColumn(
        "quantity",
        F.col("quantity").cast("int")
    )
    .withColumn(
        "unit_price",
        F.col("unit_price").cast("decimal(12,2)")
    )
    .withColumn(
        "discount_pct",
        F.col("discount_pct").cast("decimal(5,2)")
    )
    .filter(
        F.col("order_id").isNotNull()
        & F.col("order_date").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & F.col("discount_pct").between(0, 100)
        & F.col("order_status").isin(valid_statuses)
    )
)

display(silver_orders)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("order_date").desc())
)

silver_orders = (
    silver_orders
    .withColumn(
        "row_num",
        F.row_number().over(window_spec)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Keep orders with valid customers and products
silver_orders = (
    silver_orders
    .join(
        silver_customers.select("customer_id"),
        "customer_id",
        "inner"
    )
    .join(
        silver_products.select("product_id"),
        "product_id",
        "inner"
    )
)

print("Clean Orders:", silver_orders.count())

# COMMAND ----------

silver_orders = (
    silver_orders
    .withColumn(
        "line_net_amount",
        F.round(
            F.col("quantity")
            * F.col("unit_price")
            * (
                1 - F.col("discount_pct") / 100
            ),
            2
        )
    )
    .withColumn(
        "order_month",
        F.date_format("order_date", "yyyy-MM")
    )
)

display(silver_orders)

# COMMAND ----------

silver_customers.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("jobs.silver.silver_customers")

silver_orders.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("jobs.silver.silver_orders")

silver_products.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("jobs.silver.silver_products")

# COMMAND ----------

print("Customers: ", silver_customers.count())
print("Orders: ", silver_orders.count())
print("Products: ", silver_products.count())