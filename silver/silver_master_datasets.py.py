# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Silver Layer - Master Tables
# MAGIC
# MAGIC This notebook transforms bronze master tables (Status, Products, Customers) into silver tables with data quality improvements and derived columns.
# MAGIC
# MAGIC **Master tables are relatively static and can be run less frequently.**

# COMMAND ----------

# DBTITLE 1,Import statements
from pyspark.sql.functions import col, when

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

# DBTITLE 1,Create schema section
# MAGIC %md
# MAGIC ## Create Silver Schema

# COMMAND ----------

# DBTITLE 1,Create silver schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

qualified_silver_schema_name = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_silver_schema_name}")

# COMMAND ----------

# DBTITLE 1,Master tables section
# MAGIC %md
# MAGIC # Master Tables

# COMMAND ----------

# DBTITLE 1,Status table section
# MAGIC %md
# MAGIC ## silver.Status (Pass Through)

# COMMAND ----------

# DBTITLE 1,Create status table
# Pass through from bronze - no transformations needed
table__bronze__status = f"{CATALOG_NAME}.{SCHEMA_NAME__BRONZE}.status"
table__silver__status = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.status"

status_df = spark.table(table__bronze__status)

status_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__silver__status)

# COMMAND ----------

# DBTITLE 1,Products table section
# MAGIC %md
# MAGIC ## silver.Products (Add Active Flag)

# COMMAND ----------

# DBTITLE 1,Create products table
# Transform products: remove deleted_at, add active boolean column
table__bronze__products = f"{CATALOG_NAME}.{SCHEMA_NAME__BRONZE}.products"
table__silver__products = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.products"

products_bronze_df = spark.table(table__bronze__products)

# Add active column and drop deleted_at
products_silver_df = products_bronze_df \
  .withColumn("active", when(col("deleted_at").isNull(), True).otherwise(False)) \
  .drop("deleted_at")

products_silver_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__silver__products)

# COMMAND ----------

# DBTITLE 1,Customers table section
# MAGIC %md
# MAGIC ## silver.Customers (Add Active Flag)

# COMMAND ----------

# DBTITLE 1,Create customers table
# Transform customers: remove deleted_at, add active boolean column
table__bronze__customers = f"{CATALOG_NAME}.{SCHEMA_NAME__BRONZE}.customers"
table__silver__customers = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.customers"

customers_bronze_df = spark.table(table__bronze__customers)

# Add active column and drop deleted_at
customers_silver_df = customers_bronze_df \
  .withColumn("active", when(col("deleted_at").isNull(), True).otherwise(False)) \
  .drop("deleted_at")

customers_silver_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__silver__customers)

# COMMAND ----------

# DBTITLE 1,Verification section
# MAGIC %md
# MAGIC # Verify Master Tables

# COMMAND ----------

# DBTITLE 1,Verify status table
# MAGIC %sql
# MAGIC SELECT type, COUNT(*) AS total_status 
# MAGIC FROM workspace.olist_silver.status 
# MAGIC GROUP BY type;

# COMMAND ----------

# DBTITLE 1,Verify products table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   active,
# MAGIC   COUNT(*) AS product_count
# MAGIC FROM workspace.olist_silver.products
# MAGIC GROUP BY active;

# COMMAND ----------

# DBTITLE 1,Verify customers table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   active,
# MAGIC   COUNT(*) AS customer_count
# MAGIC FROM workspace.olist_silver.customers
# MAGIC GROUP BY active;

# COMMAND ----------

