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

# DBTITLE 1,Query 2 Header
# MAGIC %md
# MAGIC ## Top 10 Products by Quantity (Last Month)
# MAGIC
# MAGIC Same filters as Query 1, showing top 10 products.

# COMMAND ----------

# DBTITLE 1,Top 10 products
# MAGIC %sql
# MAGIC WITH monthly_sales AS (
# MAGIC     SELECT 
# MAGIC         oi.product_sku,
# MAGIC         SUM(oi.quantity) AS total_quantity_sold,
# MAGIC         COUNT(DISTINCT oi.order_id) AS number_of_orders,
# MAGIC         SUM(oi.quantity * oi.unit_price) AS total_revenue,
# MAGIC         ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
# MAGIC     FROM workspace.olist_gold_dlt.fact_order_items oi
# MAGIC     JOIN workspace.olist_gold_dlt.fact_orders o 
# MAGIC         ON oi.order_id = o.order_id
# MAGIC         AND o.__CURRENT = TRUE  -- SCD Type 2: current order versions only
# MAGIC     WHERE 
# MAGIC         oi.__CURRENT = TRUE  -- SCD Type 2: current item versions only
# MAGIC         AND o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC         AND o.order_date < CURRENT_DATE()
# MAGIC         AND o.order_status != 'Cancelled'
# MAGIC         AND oi.item_status != 'Cancelled'
# MAGIC     GROUP BY oi.product_sku
# MAGIC )
# MAGIC SELECT 
# MAGIC     p.product_sku,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     s.total_quantity_sold,
# MAGIC     s.number_of_orders,
# MAGIC     s.total_revenue,
# MAGIC     s.avg_unit_price
# MAGIC FROM monthly_sales s
# MAGIC JOIN workspace.olist_gold_dlt.dim_products p 
# MAGIC     ON s.product_sku = p.product_sku
# MAGIC ORDER BY s.total_quantity_sold DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Query 3 Header
# MAGIC %md
# MAGIC ## Sales Breakdown by Category (Last Month)
# MAGIC
# MAGIC Aggregate sales by product category.

# COMMAND ----------

# DBTITLE 1,Sales by category
# MAGIC %sql
# MAGIC WITH monthly_item_sales AS (
# MAGIC     SELECT 
# MAGIC         oi.product_sku,
# MAGIC         SUM(oi.quantity) AS quantity_sold,
# MAGIC         COUNT(DISTINCT oi.order_id) AS num_orders,
# MAGIC         SUM(oi.quantity * oi.unit_price) AS revenue,
# MAGIC         AVG(oi.unit_price) AS avg_price
# MAGIC     FROM workspace.olist_gold_dlt.fact_order_items oi
# MAGIC     JOIN workspace.olist_gold_dlt.fact_orders o 
# MAGIC         ON oi.order_id = o.order_id
# MAGIC         AND o.__CURRENT = TRUE  -- SCD Type 2: current order versions only
# MAGIC     WHERE 
# MAGIC         oi.__CURRENT = TRUE  -- SCD Type 2: current item versions only
# MAGIC         AND o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC         AND o.order_date < CURRENT_DATE()
# MAGIC         AND o.order_status != 'Cancelled'
# MAGIC         AND oi.item_status != 'Cancelled'
# MAGIC     GROUP BY oi.product_sku
# MAGIC )
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     COUNT(DISTINCT p.product_sku) AS unique_products,
# MAGIC     SUM(s.quantity_sold) AS total_quantity_sold,
# MAGIC     SUM(s.num_orders) AS number_of_orders,
# MAGIC     SUM(s.revenue) AS total_revenue,
# MAGIC     ROUND(AVG(s.avg_price), 2) AS avg_unit_price
# MAGIC FROM monthly_item_sales s
# MAGIC JOIN workspace.olist_gold_dlt.dim_products p 
# MAGIC     ON s.product_sku = p.product_sku
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Optimized Query Section
# MAGIC %md
# MAGIC ---
# MAGIC ## Optimized Queries Using Year/Month Partitioning
# MAGIC
# MAGIC **New Columns:** `order_year` and `order_month` added to `fact_orders`
# MAGIC
# MAGIC **Benefits:**
# MAGIC - **Partition Pruning**: Database can skip entire partitions when filtering
# MAGIC - **Faster Queries**: Especially on large datasets with time-based partitions
# MAGIC - **Integer Comparison**: Year/month comparisons are more efficient than date ranges
# MAGIC
# MAGIC **Use Case:** When querying for a specific calendar month (e.g., "previous month" or "June 2026")
# MAGIC
# MAGIC **Note:** For rolling windows like "last 30 days", the original `order_date` range filter is still appropriate.
# MAGIC
# MAGIC **SCD Type 2 Reminder:** Always include `WHERE __CURRENT = TRUE` for current state queries.

# COMMAND ----------

# DBTITLE 1,Optimized: Previous month top products (DLT)
# MAGIC %sql
# MAGIC -- Optimized query for PREVIOUS CALENDAR MONTH using year/month columns (DLT with SCD Type 2)
# MAGIC WITH previous_month AS (
# MAGIC     SELECT 
# MAGIC         YEAR(ADD_MONTHS(CURRENT_DATE(), -1)) AS prev_year,
# MAGIC         MONTH(ADD_MONTHS(CURRENT_DATE(), -1)) AS prev_month
# MAGIC ),
# MAGIC monthly_sales_optimized AS (
# MAGIC     SELECT 
# MAGIC         oi.product_sku,
# MAGIC         SUM(oi.quantity) AS total_quantity_sold,
# MAGIC         COUNT(DISTINCT oi.order_id) AS number_of_orders,
# MAGIC         SUM(oi.quantity * oi.unit_price) AS total_revenue,
# MAGIC         ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
# MAGIC     FROM workspace.olist_gold_dlt.fact_order_items oi
# MAGIC     JOIN workspace.olist_gold_dlt.fact_orders o 
# MAGIC         ON oi.order_id = o.order_id
# MAGIC         AND o.__CURRENT = TRUE  -- SCD Type 2: current order versions only
# MAGIC     CROSS JOIN previous_month pm
# MAGIC     WHERE 
# MAGIC         oi.__CURRENT = TRUE  -- SCD Type 2: current item versions only
# MAGIC         -- Partition pruning: uses year/month columns for efficient filtering
# MAGIC         AND o.order_year = pm.prev_year
# MAGIC         AND o.order_month = pm.prev_month
# MAGIC         AND o.order_status != 'Cancelled'
# MAGIC         AND oi.item_status != 'Cancelled'
# MAGIC     GROUP BY oi.product_sku
# MAGIC )
# MAGIC SELECT 
# MAGIC     p.product_sku,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     s.total_quantity_sold,
# MAGIC     s.number_of_orders,
# MAGIC     s.total_revenue,
# MAGIC     s.avg_unit_price
# MAGIC FROM monthly_sales_optimized s
# MAGIC JOIN workspace.olist_gold_dlt.dim_products p 
# MAGIC     ON s.product_sku = p.product_sku
# MAGIC ORDER BY s.total_quantity_sold DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Optimized: Previous month category sales (DLT)
# MAGIC %sql
# MAGIC -- Optimized query for PREVIOUS CALENDAR MONTH sales by category (DLT with SCD Type 2)
# MAGIC WITH previous_month AS (
# MAGIC     SELECT 
# MAGIC         YEAR(ADD_MONTHS(CURRENT_DATE(), -1)) AS prev_year,
# MAGIC         MONTH(ADD_MONTHS(CURRENT_DATE(), -1)) AS prev_month
# MAGIC ),
# MAGIC monthly_item_sales_optimized AS (
# MAGIC     SELECT 
# MAGIC         oi.product_sku,
# MAGIC         SUM(oi.quantity) AS quantity_sold,
# MAGIC         COUNT(DISTINCT oi.order_id) AS num_orders,
# MAGIC         SUM(oi.quantity * oi.unit_price) AS revenue,
# MAGIC         AVG(oi.unit_price) AS avg_price
# MAGIC     FROM workspace.olist_gold_dlt.fact_order_items oi
# MAGIC     JOIN workspace.olist_gold_dlt.fact_orders o 
# MAGIC         ON oi.order_id = o.order_id
# MAGIC         AND o.__CURRENT = TRUE  -- SCD Type 2: current order versions only
# MAGIC     CROSS JOIN previous_month pm
# MAGIC     WHERE 
# MAGIC         oi.__CURRENT = TRUE  -- SCD Type 2: current item versions only
# MAGIC         -- Partition pruning: uses year/month columns for efficient filtering
# MAGIC         AND o.order_year = pm.prev_year
# MAGIC         AND o.order_month = pm.prev_month
# MAGIC         AND o.order_status != 'Cancelled'
# MAGIC         AND oi.item_status != 'Cancelled'
# MAGIC     GROUP BY oi.product_sku
# MAGIC )
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     COUNT(DISTINCT p.product_sku) AS unique_products,
# MAGIC     SUM(s.quantity_sold) AS total_quantity_sold,
# MAGIC     SUM(s.num_orders) AS number_of_orders,
# MAGIC     SUM(s.revenue) AS total_revenue,
# MAGIC     ROUND(AVG(s.avg_price), 2) AS avg_unit_price
# MAGIC FROM monthly_item_sales_optimized s
# MAGIC JOIN workspace.olist_gold_dlt.dim_products p 
# MAGIC     ON s.product_sku = p.product_sku
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

