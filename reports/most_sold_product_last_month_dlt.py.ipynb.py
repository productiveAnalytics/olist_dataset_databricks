# Databricks notebook source
# DBTITLE 1,Report Overview
# MAGIC %md
# MAGIC # Most Sold Product in the Last Month - DLT Report
# MAGIC
# MAGIC **Data Source:** `workspace.olist_gold_dlt` (DLT pipeline with SCD Type 2)
# MAGIC
# MAGIC This report queries the DLT gold layer which implements:
# MAGIC * **SCD Type 2** for fact tables (historical tracking)
# MAGIC * **SCD Type 1** for dimension tables (current state only)
# MAGIC
# MAGIC **Important:** Must filter `__CURRENT = TRUE` for SCD Type 2 tables to get current state.
# MAGIC
# MAGIC **Report Queries:**
# MAGIC 1. Most sold product in the last month
# MAGIC 2. Top 10 products by quantity
# MAGIC 3. Sales breakdown by category

# COMMAND ----------

# DBTITLE 1,Query 1 Header
# MAGIC %md
# MAGIC ## Query 1: Most Sold Product (Last Month)
# MAGIC
# MAGIC Filters:
# MAGIC * Last 30 days (excluding today)
# MAGIC * Non-cancelled orders only
# MAGIC * Delivered items only
# MAGIC * **Current versions only** (`__CURRENT = TRUE`)

# COMMAND ----------

# DBTITLE 1,Most sold product last month
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.product_sku,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     SUM(oi.quantity) AS total_quantity_sold,
# MAGIC     COUNT(DISTINCT oi.order_id) AS number_of_orders,
# MAGIC     SUM(oi.quantity * oi.unit_price) AS total_revenue,
# MAGIC     ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
# MAGIC FROM workspace.olist_gold_dlt.fact_order_items oi
# MAGIC JOIN workspace.olist_gold_dlt.fact_orders o 
# MAGIC     ON oi.order_id = o.order_id
# MAGIC     AND o.__CURRENT = TRUE  -- Critical: Only current order versions
# MAGIC JOIN workspace.olist_gold_dlt.dim_products p 
# MAGIC     ON oi.product_sku = p.product_sku
# MAGIC WHERE 
# MAGIC     oi.__CURRENT = TRUE  -- Critical: Only current item versions
# MAGIC     AND o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC     AND o.order_date < CURRENT_DATE()
# MAGIC     AND o.order_status != 'Cancelled'
# MAGIC     AND oi.item_status = 'Delivered'
# MAGIC GROUP BY 
# MAGIC     p.product_sku, 
# MAGIC     p.product_name, 
# MAGIC     p.category
# MAGIC ORDER BY total_quantity_sold DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# DBTITLE 1,Query 2 Header
# MAGIC %md
# MAGIC ## Query 2: Top 10 Products by Quantity (Last Month)
# MAGIC
# MAGIC Same filters as Query 1, showing top 10 products.

# COMMAND ----------

# DBTITLE 1,Top 10 products
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.product_sku,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     SUM(oi.quantity) AS total_quantity_sold,
# MAGIC     COUNT(DISTINCT oi.order_id) AS number_of_orders,
# MAGIC     SUM(oi.quantity * oi.unit_price) AS total_revenue,
# MAGIC     ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
# MAGIC FROM workspace.olist_gold_dlt.fact_order_items oi
# MAGIC JOIN workspace.olist_gold_dlt.fact_orders o 
# MAGIC     ON oi.order_id = o.order_id
# MAGIC     AND o.__CURRENT = TRUE
# MAGIC JOIN workspace.olist_gold_dlt.dim_products p 
# MAGIC     ON oi.product_sku = p.product_sku
# MAGIC WHERE 
# MAGIC     oi.__CURRENT = TRUE
# MAGIC     AND o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC     AND o.order_date < CURRENT_DATE()
# MAGIC     AND o.order_status != 'Cancelled'
# MAGIC     AND oi.item_status = 'Delivered'
# MAGIC GROUP BY 
# MAGIC     p.product_sku, 
# MAGIC     p.product_name, 
# MAGIC     p.category
# MAGIC ORDER BY total_quantity_sold DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Query 3 Header
# MAGIC %md
# MAGIC ## Query 3: Sales Breakdown by Category (Last Month)
# MAGIC
# MAGIC Aggregate sales by product category.

# COMMAND ----------

# DBTITLE 1,Sales by category
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     COUNT(DISTINCT p.product_sku) AS unique_products,
# MAGIC     SUM(oi.quantity) AS total_quantity_sold,
# MAGIC     COUNT(DISTINCT oi.order_id) AS number_of_orders,
# MAGIC     SUM(oi.quantity * oi.unit_price) AS total_revenue,
# MAGIC     ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
# MAGIC FROM workspace.olist_gold_dlt.fact_order_items oi
# MAGIC JOIN workspace.olist_gold_dlt.fact_orders o 
# MAGIC     ON oi.order_id = o.order_id
# MAGIC     AND o.__CURRENT = TRUE
# MAGIC JOIN workspace.olist_gold_dlt.dim_products p 
# MAGIC     ON oi.product_sku = p.product_sku
# MAGIC WHERE 
# MAGIC     oi.__CURRENT = TRUE
# MAGIC     AND o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC     AND o.order_date < CURRENT_DATE()
# MAGIC     AND o.order_status != 'Cancelled'
# MAGIC     AND oi.item_status = 'Delivered'
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,SCD Type 2 Notes
# MAGIC %md
# MAGIC ## Important Notes: Querying SCD Type 2 Tables
# MAGIC
# MAGIC **Critical Filter:** Always include `WHERE __CURRENT = TRUE` when querying current state from SCD Type 2 tables.
# MAGIC
# MAGIC **SCD Type 2 Columns:**
# MAGIC * `__START_AT` - When this version became active
# MAGIC * `__END_AT` - When this version became inactive (NULL for current)
# MAGIC * `__CURRENT` - Boolean flag (TRUE = current, FALSE = historical)
# MAGIC
# MAGIC **Why This Matters:**
# MAGIC * Without `__CURRENT = TRUE`, queries return ALL versions (current + historical)
# MAGIC * This leads to duplicate records and incorrect aggregations
# MAGIC * Historical versions track changes over time (e.g., status updates, price changes)
# MAGIC
# MAGIC **Example - Querying History:**
# MAGIC ```sql
# MAGIC -- See all historical versions of an order
# MAGIC SELECT order_id, order_status, __START_AT, __END_AT, __CURRENT
# MAGIC FROM workspace.olist_gold_dlt.fact_orders
# MAGIC WHERE order_id = 123
# MAGIC ORDER BY __START_AT;
# MAGIC ```

# COMMAND ----------

