# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Gold Layer - Dimension: Products
# MAGIC
# MAGIC This notebook creates the `dim_products` dimension table in the gold layer.
# MAGIC
# MAGIC **Source:** workspace.olist_silver.products
# MAGIC
# MAGIC **Business Logic:**
# MAGIC - Product catalog details
# MAGIC - Active products only

# COMMAND ----------

# DBTITLE 1,Import statements
from pyspark.sql.functions import col

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
# MAGIC ## Transform: dim_products

# COMMAND ----------

# DBTITLE 1,Create dim_products
# Read silver products table
table__silver__products = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.products"
table__gold__dim_products = f"{CATALOG_NAME}.{SCHEMA_NAME__GOLD}.dim_products"

products_df = spark.table(table__silver__products)

# Transform: Filter active products and select required columns
dim_products_df = products_df \
  .filter(col("active") == True) \
  .select(
    col("product_sku"),
    col("product_name"),
    col("category"),
    col("price")
  )

print(f"Total products: {products_df.count()}")
print(f"Active products: {dim_products_df.count()}")

# Write to gold layer
dim_products_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__gold__dim_products)

print(f"✓ Created {table__gold__dim_products}")

# COMMAND ----------

# DBTITLE 1,Verification section
# MAGIC %md
# MAGIC ## Verify dim_products

# COMMAND ----------

# DBTITLE 1,Verify table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   product_sku,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   price
# MAGIC FROM workspace.olist_gold.dim_products
# MAGIC ORDER BY category, product_name
# MAGIC LIMIT 10;

# COMMAND ----------

