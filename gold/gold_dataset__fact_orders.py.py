# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Gold Layer - Fact: Orders
# MAGIC
# MAGIC This notebook creates the `fact_orders` fact table in the gold layer.
# MAGIC
# MAGIC **Sources:** 
# MAGIC - workspace.olist_silver.orders__active
# MAGIC - workspace.olist_silver.orders__inactive
# MAGIC
# MAGIC **Business Logic:**
# MAGIC - High-level order metadata
# MAGIC - Includes both active and cancelled orders for historical completeness
# MAGIC - Order date extracted from ordered_at timestamp

# COMMAND ----------

# DBTITLE 1,Import statements
from pyspark.sql.functions import col, to_date

# COMMAND ----------

# DBTITLE 1,Constants
# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

# DBTITLE 1,Define constants
CATALOG_NAME = 'workspace'
SCHEMA_NAME__SILVER = 'olist_silver'
SCHEMA_NAME__GOLD = 'olist_gold'

# COMMAND ----------

# DBTITLE 1,Create schema section
# MAGIC %md
# MAGIC ## Create Gold Schema

# COMMAND ----------

# DBTITLE 1,Create gold schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

qualified_gold_schema_name = f"{CATALOG_NAME}.{SCHEMA_NAME__GOLD}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_gold_schema_name}")

print(f"✓ Schema created: {qualified_gold_schema_name}")

# COMMAND ----------

# DBTITLE 1,Transform section
# MAGIC %md
# MAGIC ## Transform: fact_orders

# COMMAND ----------

# DBTITLE 1,Create fact_orders
# Read silver orders tables (both active and inactive)
table__silver__orders_active = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.orders__active"
table__silver__orders_inactive = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.orders__inactive"
table__gold__fact_orders = f"{CATALOG_NAME}.{SCHEMA_NAME__GOLD}.fact_orders"

orders_active_df = spark.table(table__silver__orders_active)
orders_inactive_df = spark.table(table__silver__orders_inactive)

# Union both active and inactive orders
orders_all_df = orders_active_df.unionByName(orders_inactive_df)

# Transform: Extract order date and select required columns
fact_orders_df = orders_all_df \
  .select(
    col("order_id"),
    col("customer_id"),
    to_date(col("ordered_at")).alias("order_date"),
    col("order_status"),
    col("total_amount"),
    col("updated_at")
  )

print(f"Active orders: {orders_active_df.count()}")
print(f"Inactive orders: {orders_inactive_df.count()}")
print(f"Total orders: {fact_orders_df.count()}")

# Write to gold layer
fact_orders_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__gold__fact_orders)

print(f"✓ Created {table__gold__fact_orders}")

# COMMAND ----------

# DBTITLE 1,Verification section
# MAGIC %md
# MAGIC ## Verify fact_orders

# COMMAND ----------

# DBTITLE 1,Verify order status distribution
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_status,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(total_amount) AS total_revenue
# MAGIC FROM workspace.olist_gold.fact_orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# DBTITLE 1,Sample records
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   order_status,
# MAGIC   total_amount
# MAGIC FROM workspace.olist_gold.fact_orders
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 5;

# COMMAND ----------

