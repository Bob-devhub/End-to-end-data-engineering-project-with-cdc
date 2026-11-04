# Fabric notebook source: nb_build_gold
# Star schema for the Direct Lake semantic model. Joins batch Silver with
# real-time Eventhouse tables exposed through OneLake availability.
#
# Default lakehouse: lh_gold. Attach lh_silver. Add a OneLake shortcut in
# lh_gold Tables/ named rt_pos_transactions -> Eventhouse table pos_transactions.

# %% [parameters]
silver = "lh_silver"

# %% dim_customer (SCD2 from Copy job -> current + history)
spark.sql(f"""
CREATE OR REPLACE TABLE dim_customer AS
SELECT
    xxhash64(customer_id, valid_from) AS customer_key,
    customer_id, customer_name, customer_category_id, delivery_city_id, credit_limit,
    valid_from, coalesce(valid_to, timestamp'9999-12-31') AS valid_to, is_current
FROM {silver}.customers
""")

# %% dim_stock_item (SCD2)
spark.sql(f"""
CREATE OR REPLACE TABLE dim_stock_item AS
SELECT
    xxhash64(stock_item_id, valid_from) AS stock_item_key,
    stock_item_id, stock_item_name, brand, unit_price, recommended_retail_price,
    valid_from, coalesce(valid_to, timestamp'9999-12-31') AS valid_to, is_current
FROM {silver}.stock_items
""")

# %% dim_store (from partner footfall feed)
spark.sql(f"""
CREATE OR REPLACE TABLE dim_store AS
SELECT store_id, max_by(store_name, footfall_date) AS store_name, max_by(region, footfall_date) AS region
FROM {silver}.store_footfall
GROUP BY store_id
""")

# %% fact_sales (online/B2B orders from OLTP) - point-in-time join to SCD2 dims
spark.sql(f"""
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    o.order_id, ol.order_line_id, o.order_date,
    dc.customer_key, ds.stock_item_key,
    ol.quantity, ol.unit_price, ol.quantity * ol.unit_price AS line_amount
FROM {silver}.orders o
JOIN {silver}.order_lines ol ON ol.order_id = o.order_id
LEFT JOIN dim_customer dc
       ON dc.customer_id = o.customer_id
      AND o.order_date >= CAST(dc.valid_from AS DATE) AND o.order_date < CAST(dc.valid_to AS DATE)
LEFT JOIN dim_stock_item ds
       ON ds.stock_item_id = ol.stock_item_id
      AND o.order_date >= CAST(ds.valid_from AS DATE) AND o.order_date < CAST(ds.valid_to AS DATE)
""")

# %% fact_pos_daily (in-store sales from the real-time path, via OneLake availability)
if spark.catalog.tableExists("rt_pos_transactions"):
    spark.sql("""
    CREATE OR REPLACE TABLE fact_pos_daily AS
    SELECT CAST(event_time AS DATE) AS sales_date, store_id, stock_item_id,
           SUM(quantity) AS units, SUM(amount) AS revenue, COUNT(DISTINCT transaction_id) AS transactions
    FROM rt_pos_transactions
    GROUP BY CAST(event_time AS DATE), store_id, stock_item_id
    """)
else:
    print("rt_pos_transactions shortcut not found - skipping fact_pos_daily")
