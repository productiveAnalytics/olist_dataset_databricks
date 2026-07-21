# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Gold Layer - Fact: Order Items
# MAGIC
# MAGIC This notebook creates the `fact_order_items` fact/bridge table in the gold layer.
# MAGIC
# MAGIC **Source:** workspace.olist_silver.order_items
# MAGIC
# MAGIC **Business Logic:**
# MAGIC - Line-item level granularity
# MAGIC - Individual item status tracking
# MAGIC - Unit price calculated from line_amount / quantity

# COMMAND ----------

# DBTITLE 1,Import statements
from pyspark.sql.functions import col, round as spark_round

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
# MAGIC ## Transform: fact_order_items

# COMMAND ----------

# DBTITLE 1,Create fact_order_items
# Read silver order_items table
table__silver__order_items = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.order_items"
table__gold__fact_order_items = f"{CATALOG_NAME}.{SCHEMA_NAME__GOLD}.fact_order_items"

order_items_df = spark.table(table__silver__order_items)

# Transform: Calculate unit_price and select required columns
fact_order_items_df = order_items_df \
  .select(
    col("order_item_id"),
    col("order_id"),
    col("product_sku"),
    col("quantity"),
    spark_round(col("line_amount") / col("quantity"), 2).alias("unit_price"),
    col("item_status"),
    col("updated_at")
  )

print(f"Total order items: {fact_order_items_df.count()}")

# Write to gold layer
fact_order_items_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__gold__fact_order_items)

print(f"✓ Created {table__gold__fact_order_items}")

# COMMAND ----------

# DBTITLE 1,Verification section
# MAGIC %md
# MAGIC ## Verify fact_order_items

# COMMAND ----------

# DBTITLE 1,Verify item status distribution
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   item_status,
# MAGIC   COUNT(*) AS item_count,
# MAGIC   SUM(quantity) AS total_quantity,
# MAGIC   SUM(quantity * unit_price) AS total_amount
# MAGIC FROM workspace.olist_gold.fact_order_items
# MAGIC GROUP BY item_status
# MAGIC ORDER BY item_count DESC;

# COMMAND ----------

# DBTITLE 1,Sample records with price calculation
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_item_id,
# MAGIC   order_id,
# MAGIC   product_sku,
# MAGIC   quantity,
# MAGIC   unit_price,
# MAGIC   ROUND(quantity * unit_price, 2) AS line_total,
# MAGIC   item_status
# MAGIC FROM workspace.olist_gold.fact_order_items
# MAGIC LIMIT 10;

# COMMAND ----------

