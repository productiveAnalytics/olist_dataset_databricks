# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Gold Layer - Dimension: Customers
# MAGIC
# MAGIC This notebook creates the `dim_customers` dimension table in the gold layer.
# MAGIC
# MAGIC **Source:** workspace.olist_silver.customers
# MAGIC
# MAGIC **Business Logic:**
# MAGIC - Customer profile details
# MAGIC - Active customers only
# MAGIC - Formatted customer name as "Last_Name, First_Name"

# COMMAND ----------

# DBTITLE 1,Import statements
from pyspark.sql.functions import col, concat, lit

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
# MAGIC ## Transform: dim_customers

# COMMAND ----------

# DBTITLE 1,Create dim_customers
# Read silver customers table
table__silver__customers = f"{CATALOG_NAME}.{SCHEMA_NAME__SILVER}.customers"
table__gold__dim_customers = f"{CATALOG_NAME}.{SCHEMA_NAME__GOLD}.dim_customers"

customers_df = spark.table(table__silver__customers)

# Transform: Filter active customers and format customer name
dim_customers_df = customers_df \
  .filter(col("active") == True) \
  .select(
    col("customer_id"),
    concat(col("last_name"), lit(", "), col("first_name")).alias("customer_name"),
    col("email"),
    col("created_at")
  )

print(f"Total customers: {customers_df.count()}")
print(f"Active customers: {dim_customers_df.count()}")

# Write to gold layer
dim_customers_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable(table__gold__dim_customers)

print(f"✓ Created {table__gold__dim_customers}")

# COMMAND ----------

# DBTITLE 1,Verification section
# MAGIC %md
# MAGIC ## Verify dim_customers

# COMMAND ----------

# DBTITLE 1,Verify table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   created_at
# MAGIC FROM workspace.olist_gold.dim_customers
# MAGIC LIMIT 5;