# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Report: Most Sold Product in the Last Month
# MAGIC
# MAGIC This notebook implements the analytical query to find the most sold product in the last 30 days.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - workspace.olist_gold.fact_order_items
# MAGIC - workspace.olist_gold.fact_orders
# MAGIC - workspace.olist_gold.dim_products
# MAGIC
# MAGIC **Business Logic:**
# MAGIC - Time filter: Last 30 days from current date
# MAGIC - Exclude cancelled orders
# MAGIC - Count items in any status EXCEPT 'Cancelled' (includes Initial, Shipped, Delivered)
# MAGIC - Products are considered "sold" even if in-transit or pending
# MAGIC - Aggregate by product to find highest quantity sold

# COMMAND ----------

# DBTITLE 1,Top 10 section
# MAGIC %md
# MAGIC ## Top 10 Most Sold Products

# COMMAND ----------

# DBTITLE 1,Top 10 products
# MAGIC %sql
# MAGIC WITH monthly_sales AS (
# MAGIC     SELECT 
# MAGIC         oi.product_sku,
# MAGIC         SUM(oi.quantity) AS total_quantity_sold,
# MAGIC         COUNT(DISTINCT oi.order_id) AS number_of_orders,
# MAGIC         SUM(oi.quantity * oi.unit_price) AS total_revenue
# MAGIC     FROM workspace.olist_gold.fact_order_items oi
# MAGIC     JOIN workspace.olist_gold.fact_orders o 
# MAGIC         ON oi.order_id = o.order_id
# MAGIC     WHERE 
# MAGIC         o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
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
# MAGIC     s.total_revenue
# MAGIC FROM monthly_sales s
# MAGIC JOIN workspace.olist_gold.dim_products p 
# MAGIC     ON s.product_sku = p.product_sku
# MAGIC ORDER BY s.total_quantity_sold DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Sales by category section
# MAGIC %md
# MAGIC ## Sales by Category (Last 30 Days)

# COMMAND ----------

# DBTITLE 1,Category breakdown
# MAGIC %sql
# MAGIC WITH monthly_item_sales AS (
# MAGIC     SELECT 
# MAGIC         oi.product_sku,
# MAGIC         SUM(oi.quantity) AS quantity_sold,
# MAGIC         COUNT(DISTINCT oi.order_id) AS num_orders,
# MAGIC         SUM(oi.quantity * oi.unit_price) AS revenue
# MAGIC     FROM workspace.olist_gold.fact_order_items oi
# MAGIC     JOIN workspace.olist_gold.fact_orders o 
# MAGIC         ON oi.order_id = o.order_id
# MAGIC     WHERE 
# MAGIC         o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC         AND o.order_date < CURRENT_DATE()
# MAGIC         AND o.order_status != 'Cancelled'
# MAGIC         AND oi.item_status != 'Cancelled'
# MAGIC     GROUP BY oi.product_sku
# MAGIC )
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     SUM(s.quantity_sold) AS total_quantity_sold,
# MAGIC     COUNT(DISTINCT p.product_sku) AS distinct_products,
# MAGIC     SUM(s.num_orders) AS number_of_orders,
# MAGIC     SUM(s.revenue) AS total_revenue
# MAGIC FROM monthly_item_sales s
# MAGIC JOIN workspace.olist_gold.dim_products p 
# MAGIC     ON s.product_sku = p.product_sku
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_quantity_sold DESC;