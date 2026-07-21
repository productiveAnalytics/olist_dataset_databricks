# Databricks notebook source
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC # Gold Layer - DLT Pipeline with SCD Patterns
# MAGIC
# MAGIC **Target Schema:** `workspace.olist_gold_dlt`
# MAGIC
# MAGIC This DLT pipeline implements the gold layer star schema with:
# MAGIC
# MAGIC **SCD Type 1 (Current State Only):**
# MAGIC - `dim_customers` - Customer dimension (overwrites on change)
# MAGIC - `dim_products` - Product dimension (overwrites on change)
# MAGIC
# MAGIC **SCD Type 2 (Historical Tracking):**
# MAGIC - `fact_orders` - Order fact with history (valid_from, valid_to, is_current)
# MAGIC - `fact_order_items` - Order items fact with history
# MAGIC
# MAGIC **Features:**
# MAGIC - Automatic dependency management
# MAGIC - Late arriving record handling
# MAGIC - Data quality expectations
# MAGIC - Incremental processing

# COMMAND ----------

# DBTITLE 1,Import DLT
import dlt
from pyspark.sql.functions import col, concat, lit, to_date, current_timestamp, when, expr, round as spark_round

# COMMAND ----------

# DBTITLE 1,Dimension Tables Section
# MAGIC %md
# MAGIC ## Dimension Tables (SCD Type 1)
# MAGIC
# MAGIC Dimensions use SCD Type 1 - only current state is maintained. Changes overwrite previous values.

# COMMAND ----------

# DBTITLE 1,dim_customers with SCD Type 1
@dlt.table(
    name="dim_customers",
    comment="Customer dimension with SCD Type 1 (current state only). Target: workspace.olist_gold_dlt.dim_customers",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
def dim_customers():
    """
    SCD Type 1: Only current state maintained.
    Active customers only with formatted names.
    """
    return (
        dlt.read("workspace.olist_silver.customers")
        .filter(col("active") == True)
        .select(
            col("customer_id"),
            concat(col("last_name"), lit(", "), col("first_name")).alias("customer_name"),
            col("email"),
            col("created_at")
        )
    )

# COMMAND ----------

# DBTITLE 1,dim_products with SCD Type 1
@dlt.table(
    name="dim_products",
    comment="Product dimension with SCD Type 1 (current state only). Target: workspace.olist_gold_dlt.dim_products",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_product_sku", "product_sku IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price >= 0")
def dim_products():
    """
    SCD Type 1: Only current state maintained.
    Active products only.
    """
    return (
        dlt.read("workspace.olist_silver.products")
        .filter(col("active") == True)
        .select(
            col("product_sku"),
            col("product_name"),
            col("category"),
            col("price")
        )
    )

# COMMAND ----------

# DBTITLE 1,Fact Tables Section
# MAGIC %md
# MAGIC ## Fact Tables (SCD Type 2)
# MAGIC
# MAGIC Fact tables use SCD Type 2 - historical versions are tracked with valid_from, valid_to, and is_current flags.

# COMMAND ----------

# DBTITLE 1,Staging view for fact_orders
@dlt.view(
    name="fact_orders_staging",
    comment="Staging view for fact_orders with transformations"
)
def fact_orders_staging():
    """
    Prepare orders data for SCD Type 2 processing.
    Only non-cancelled orders (from orders__active).
    """
    return (
        dlt.read("workspace.olist_silver.orders__active")
        .select(
            col("order_id"),
            col("customer_id"),
            to_date(col("ordered_at")).alias("order_date"),
            col("order_status"),
            col("total_amount"),
            col("updated_at")
        )
    )

# COMMAND ----------

# DBTITLE 1,fact_orders with SCD Type 2
# Apply SCD Type 2 for fact_orders
dlt.create_streaming_table(
    name="fact_orders",
    comment="Order fact table with SCD Type 2 (historical tracking). Target: workspace.olist_gold_dlt.fact_orders",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

dlt.apply_changes(
    target="fact_orders",
    source="fact_orders_staging",
    keys=["order_id"],
    sequence_by="updated_at",
    stored_as_scd_type="2",
    track_history_column_list=["order_status", "total_amount"],
    except_column_list=[],
    ignore_null_updates=False
)

# COMMAND ----------

# DBTITLE 1,Staging view for fact_order_items
@dlt.view(
    name="fact_order_items_staging",
    comment="Staging view for fact_order_items with transformations"
)
def fact_order_items_staging():
    """
    Prepare order items data for SCD Type 2 processing.
    Calculate unit_price from line_amount / quantity.
    """
    return (
        dlt.read("workspace.olist_silver.order_items")
        .select(
            col("order_item_id"),
            col("order_id"),
            col("product_sku"),
            col("quantity"),
            spark_round(col("line_amount") / col("quantity"), 2).alias("unit_price"),
            col("item_status"),
            col("updated_at")
        )
    )

# COMMAND ----------

# DBTITLE 1,fact_order_items with SCD Type 2
# Apply SCD Type 2 for fact_order_items
dlt.create_streaming_table(
    name="fact_order_items",
    comment="Order items fact table with SCD Type 2 (historical tracking). Target: workspace.olist_gold_dlt.fact_order_items",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

dlt.apply_changes(
    target="fact_order_items",
    source="fact_order_items_staging",
    keys=["order_item_id"],
    sequence_by="updated_at",
    stored_as_scd_type="2",
    track_history_column_list=["item_status", "quantity", "unit_price"],
    except_column_list=[],
    ignore_null_updates=False
)

# COMMAND ----------

# DBTITLE 1,Data Quality Section
# MAGIC %md
# MAGIC ## Data Quality & Late Arriving Records
# MAGIC
# MAGIC **Late Arriving Records:**
# MAGIC - `dlt.apply_changes()` uses `sequence_by="updated_at"` to handle late arrivals
# MAGIC - Records arriving out-of-order are correctly placed in history based on timestamp
# MAGIC - SCD Type 2 tracks all changes with `__START_AT` and `__END_AT` columns
# MAGIC
# MAGIC **Data Quality:**
# MAGIC - Dimensions have validation rules (expect_or_drop)
# MAGIC - Facts use streaming tables for incremental processing
# MAGIC - Auto-optimization enabled for Delta tables

# COMMAND ----------

