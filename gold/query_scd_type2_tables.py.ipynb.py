# Databricks notebook source
# DBTITLE 1,Guide Overview
# MAGIC %md
# MAGIC # Querying SCD Type 2 Tables - User Guide
# MAGIC
# MAGIC This notebook demonstrates how to query SCD Type 2 fact tables created by the DLT pipeline.
# MAGIC
# MAGIC **SCD Type 2 Columns Added by DLT:**
# MAGIC - `__START_AT` - Timestamp when this version became active
# MAGIC - `__END_AT` - Timestamp when this version became inactive (NULL for current version)
# MAGIC - `__CURRENT` - Boolean flag (TRUE for current version, FALSE for historical)
# MAGIC
# MAGIC **Tables with SCD Type 2:**
# MAGIC - `workspace.olist_gold.fact_orders`
# MAGIC - `workspace.olist_gold.fact_order_items`

# COMMAND ----------

# DBTITLE 1,Current State Queries
# MAGIC %md
# MAGIC ## Querying Current State Only
# MAGIC
# MAGIC For most analytics, you want only the current version of each record.

# COMMAND ----------

# DBTITLE 1,Current orders only
# MAGIC %sql
# MAGIC -- Get current state of all orders (no history)
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   order_status,
# MAGIC   total_amount,
# MAGIC   updated_at
# MAGIC FROM workspace.olist_gold.fact_orders
# MAGIC WHERE __CURRENT = TRUE
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Current order items only
# MAGIC %sql
# MAGIC -- Get current state of all order items (no history)
# MAGIC SELECT 
# MAGIC   order_item_id,
# MAGIC   order_id,
# MAGIC   product_sku,
# MAGIC   quantity,
# MAGIC   unit_price,
# MAGIC   item_status,
# MAGIC   updated_at
# MAGIC FROM workspace.olist_gold.fact_order_items
# MAGIC WHERE __CURRENT = TRUE
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Historical Queries
# MAGIC %md
# MAGIC ## Querying Historical Versions
# MAGIC
# MAGIC View how records changed over time.

# COMMAND ----------

# DBTITLE 1,Order history
# MAGIC %sql
# MAGIC -- View all historical versions of a specific order
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   order_status,
# MAGIC   total_amount,
# MAGIC   __START_AT,
# MAGIC   __END_AT,
# MAGIC   __CURRENT,
# MAGIC   CASE 
# MAGIC     WHEN __CURRENT = TRUE THEN 'Current Version'
# MAGIC     ELSE 'Historical Version'
# MAGIC   END AS version_type
# MAGIC FROM workspace.olist_gold.fact_orders
# MAGIC WHERE order_id = 1
# MAGIC ORDER BY __START_AT;

# COMMAND ----------

# DBTITLE 1,Order item status changes
# MAGIC %sql
# MAGIC -- Track status changes for an order item
# MAGIC SELECT 
# MAGIC   order_item_id,
# MAGIC   item_status,
# MAGIC   quantity,
# MAGIC   unit_price,
# MAGIC   __START_AT AS status_changed_at,
# MAGIC   __END_AT AS status_valid_until,
# MAGIC   __CURRENT AS is_current
# MAGIC FROM workspace.olist_gold.fact_order_items
# MAGIC WHERE order_item_id = 1
# MAGIC ORDER BY __START_AT;

# COMMAND ----------

# DBTITLE 1,Point-in-Time Queries
# MAGIC %md
# MAGIC ## Point-in-Time Analysis
# MAGIC
# MAGIC Query what the data looked like at a specific point in time.

# COMMAND ----------

# DBTITLE 1,Orders as of date
# MAGIC %sql
# MAGIC -- Get state of orders as of a specific date
# MAGIC -- Replace '2026-07-01' with your desired date
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_status,
# MAGIC   total_amount,
# MAGIC   __START_AT AS valid_from,
# MAGIC   __END_AT AS valid_to
# MAGIC FROM workspace.olist_gold.fact_orders
# MAGIC WHERE 
# MAGIC   __START_AT <= '2026-07-01'
# MAGIC   AND (__END_AT > '2026-07-01' OR __END_AT IS NULL)
# MAGIC ORDER BY order_id
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Analytical Queries
# MAGIC %md
# MAGIC ## Most Sold Product (Using Current State)
# MAGIC
# MAGIC Updated version of the report query using SCD Type 2 tables.

# COMMAND ----------

# DBTITLE 1,Most sold product with SCD Type 2
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.product_sku,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     SUM(oi.quantity) AS total_quantity_sold,
# MAGIC     COUNT(DISTINCT oi.order_id) AS number_of_orders,
# MAGIC     SUM(oi.quantity * oi.unit_price) AS total_revenue,
# MAGIC     ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
# MAGIC FROM workspace.olist_gold.fact_order_items oi
# MAGIC JOIN workspace.olist_gold.fact_orders o 
# MAGIC     ON oi.order_id = o.order_id
# MAGIC     AND o.__CURRENT = TRUE  -- Only current order versions
# MAGIC JOIN workspace.olist_gold.dim_products p 
# MAGIC     ON oi.product_sku = p.product_sku
# MAGIC WHERE 
# MAGIC     oi.__CURRENT = TRUE  -- Only current item versions
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

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## Best Practices for SCD Type 2 Queries
# MAGIC
# MAGIC **1. Always filter on `__CURRENT = TRUE` for current-state analytics**
# MAGIC ```sql
# MAGIC WHERE __CURRENT = TRUE
# MAGIC ```
# MAGIC
# MAGIC **2. For point-in-time queries, use range conditions**
# MAGIC ```sql
# MAGIC WHERE __START_AT <= @as_of_date 
# MAGIC   AND (__END_AT > @as_of_date OR __END_AT IS NULL)
# MAGIC ```
# MAGIC
# MAGIC **3. When joining SCD Type 2 tables, apply `__CURRENT` filter to both**
# MAGIC ```sql
# MAGIC FROM fact_orders o
# MAGIC JOIN fact_order_items oi 
# MAGIC   ON o.order_id = oi.order_id
# MAGIC   AND o.__CURRENT = TRUE
# MAGIC   AND oi.__CURRENT = TRUE
# MAGIC ```
# MAGIC
# MAGIC **4. Exclude system columns from SELECT when not needed**
# MAGIC ```sql
# MAGIC SELECT order_id, customer_id, order_date
# MAGIC -- Don't include __START_AT, __END_AT, __CURRENT unless analyzing history
# MAGIC ```

# COMMAND ----------

