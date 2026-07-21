# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Silver Layer - Transaction Tables
# MAGIC
# MAGIC This notebook transforms bronze transaction tables (Orders, Order_Items) into silver tables with data quality improvements and derived columns.
# MAGIC
# MAGIC **Transaction tables are frequently updated and can be run more often than master tables.**

# COMMAND ----------

# DBTITLE 1,Import statements
from pyspark.sql.functions import col, when, to_utc_timestamp

# COMMAND ----------

# DBTITLE 1,Constants section
# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

# DBTITLE 1,Define constants
CATALOG_NAME = 'workspace'
SCHEMA_NAME__BRONZE = 'olist_bronze'
SCHEMA_NAME__SILVER = 'olist_silver'

# COMMAND ----------

# DBTITLE 1,Transaction tables section
# MAGIC %md
# MAGIC # Transaction Tables

# COMMAND ----------

# DBTITLE 1,Orders table section
# MAGIC %md
# MAGIC ## silver.Orders (Split by Status + UTC Conversion)

# COMMAND ----------

# DBTITLE 1,Create orders tables
# Transform orders: convert datetime to UTC and split by status
table__bronze__orders = f"{CATALOG_NAME}.{SCHEMA_NAME__BRONZE}.orders"
table__silver__orders_active = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.orders__active"
table__silver__orders_inactive = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.orders__inactive"

orders_bronze_df = spark.table(table__bronze__orders)

# Get datetime columns to convert to UTC
datetime_columns = [field.name for field in orders_bronze_df.schema.fields 
                   if str(field.dataType) == 'TimestampType']

print(f"Converting datetime columns to UTC: {datetime_columns}")

# Convert all datetime columns to UTC
orders_df = orders_bronze_df
for col_name in datetime_columns:
    orders_df = orders_df.withColumn(col_name, to_utc_timestamp(col(col_name), 'UTC'))

# Split into active and inactive tables
orders_active_df = orders_df.filter(col("order_status") != "Cancelled")
orders_inactive_df = orders_df.filter(col("order_status") == "Cancelled")

# Write active orders
orders_active_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__silver__orders_active)

# Write inactive orders
orders_inactive_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__silver__orders_inactive)

print(f"✓ Created {table__silver__orders_active}: {orders_active_df.count()} rows")
print(f"✓ Created {table__silver__orders_inactive}: {orders_inactive_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Order items table section
# MAGIC %md
# MAGIC ## silver.Order_Items (Add Discounted Flag + UTC Conversion)

# COMMAND ----------

# DBTITLE 1,Create order items table
# Transform order_items: convert datetime to UTC and add discounted boolean
table__bronze__order_items = f"{CATALOG_NAME}.{SCHEMA_NAME__BRONZE}.order_items"
table__silver__order_items = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.order_items"

order_items_bronze_df = spark.table(table__bronze__order_items)

# Get datetime columns to convert to UTC
datetime_columns = [field.name for field in order_items_bronze_df.schema.fields 
                   if str(field.dataType) == 'TimestampType']

print(f"Converting datetime columns to UTC: {datetime_columns}")

# Convert all datetime columns to UTC
order_items_df = order_items_bronze_df
for col_name in datetime_columns:
    order_items_df = order_items_df.withColumn(col_name, to_utc_timestamp(col(col_name), 'UTC'))

# Add discounted column and drop discount_applied
order_items_silver_df = order_items_df \
  .withColumn("discounted", when(col("discount_applied") > 0, True).otherwise(False)) \
  .drop("discount_applied")

order_items_silver_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__silver__order_items)

print(f"✓ Created {table__silver__order_items}: {order_items_silver_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Verify transaction tables section
# MAGIC %md
# MAGIC # Verify Transaction Tables

# COMMAND ----------

# DBTITLE 1,Verify orders active table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_status,
# MAGIC   COUNT(*) AS order_count
# MAGIC FROM workspace.olist_silver.orders__active
# MAGIC GROUP BY order_status;

# COMMAND ----------

# DBTITLE 1,Verify orders inactive table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_status,
# MAGIC   COUNT(*) AS order_count
# MAGIC FROM workspace.olist_silver.orders__inactive
# MAGIC GROUP BY order_status;

# COMMAND ----------

# DBTITLE 1,Verify order items table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   discounted,
# MAGIC   COUNT(*) AS item_count
# MAGIC FROM workspace.olist_silver.order_items
# MAGIC GROUP BY discounted;

# COMMAND ----------

