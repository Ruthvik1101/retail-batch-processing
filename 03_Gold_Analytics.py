# Databricks notebook source
from pyspark.sql import functions as F

df_customers = spark.table("jobs.silver.silver_customers")
df_orders = spark.table("jobs.silver.silver_orders")
df_products = spark.table("jobs.silver.silver_products")

print("Total Silver customer Records: ", df_customers.count())
print("Total Silver order Records: ", df_orders.count())
print("Total Silver product Records: ", df_products.count())

# COMMAND ----------

sales_details = (
    df_orders
    .join(df_customers, "customer_id", "left")
    .join(df_products, "product_id", "left")
)

display(sales_details)

# COMMAND ----------

# DBTITLE 1,delievered orders
delivered_orders = sales_details.filter(
    df_orders.order_status == "Delivered"
)

display(delivered_orders)

# COMMAND ----------

print("Total Joined Orders:", sales_details.count())

print("Delivered Orders:", delivered_orders.count())

# COMMAND ----------

# DBTITLE 1,Daily sales
from pyspark.sql.functions import sum, count, round, desc

gold_daily_sales = (
    delivered_orders.groupBy("order_date")\
    .agg(
        count("order_id").alias("total_orders"),
        sum("line_net_amount").alias("total_revenue")
    )
)

display(gold_daily_sales)

# COMMAND ----------

# DBTITLE 1,monthly sales
gold_monthly_sales = (
    delivered_orders.groupBy("order_month")\
        .agg(
            count("order_id").alias("Total_Orders"),
            sum("quantity").alias("Total_Quantity"),
            sum("line_net_amount").alias("Total_Revenue")
        )
    .orderBy("order_month")
)

display(gold_monthly_sales)

# COMMAND ----------

# DBTITLE 1,category sales
gold_category_sales = (
    delivered_orders
    .groupBy("category")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum("line_net_amount"), 2).alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
)

display(gold_category_sales)

# COMMAND ----------

# DBTITLE 1,customer summary
gold_customer_summary = (
    delivered_orders
    .groupBy(
        "customer_id",
        "first_name",
        "last_name",
        "city",
        "customer_segment"
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum("line_net_amount"), 2).alias("total_spent")
    )
    .orderBy(desc("total_spent"))
)

display(gold_customer_summary)

# COMMAND ----------

gold_daily_sales.write.mode("overwrite").saveAsTable(
    "workspace.gold.gold_daily_sales"
)

gold_monthly_sales.write.mode("overwrite").saveAsTable(
    "workspace.gold.gold_monthly_sales"
)

gold_category_sales.write.mode("overwrite").saveAsTable(
    "workspace.gold.gold_category_sales"
)

gold_customer_summary.write.mode("overwrite").saveAsTable(
    "workspace.gold.gold_customer_summary"
)

print("All Gold tables created successfully")