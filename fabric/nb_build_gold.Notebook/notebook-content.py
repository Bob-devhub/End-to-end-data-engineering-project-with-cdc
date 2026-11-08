# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "20984702-4720-4e95-a129-39600d5cafff",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "9d04974e-4eed-49f7-8f94-d136f1e83a45",
# META       "known_lakehouses": [
# META         {
# META           "id": "20984702-4720-4e95-a129-39600d5cafff"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1f312b0b-0741-afc0-43c6-d007484d8eff",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "acf44fea-1768-4276-afe7-df4cb3ab2152",
# META       "known_warehouses": [
# META         {
# META           "id": "acf44fea-1768-4276-afe7-df4cb3ab2152",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric notebook source: nb_build_gold
# Silver -> Gold star schema for the Direct Lake semantic model. Full rebuild each run.
#
# Default lakehouse: lh_gold (schema-enabled; tables go to dbo). Attach lh_silver as a second lakehouse.
# Environment: env_cdc_framework (Runtime 2.0 / Spark 4).
#
# SCD2: Copy job's valid_from/valid_to are LOAD times. Effective ranges use the source's business
# change time (temporal ValidFrom -> source_valid_from): a version is valid until the next version's
# change time. The first version of each key is stretched back to 1900 so history that predates it
# (e.g. old orders) still finds a version. Unmatched facts point at the -1 unknown member.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# %% [parameters]
silver = "lh_silver.dbo"   # schema-enabled lakehouse: lakehouse.schema
pipeline_run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
import json

tables: dict[str, int] = {}


def build(name: str, sql: str) -> None:
    spark.sql(f"CREATE OR REPLACE TABLE {name} AS {sql}")  # noqa: F821 - Fabric runtime
    tables[name] = spark.table(name).count()  # noqa: F821
    print(f"{name}: {tables[name]} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% dim_date - covers every date used by a fact, through today
build("dim_date", f"""
WITH bounds AS (
    SELECT min(d) AS lo, greatest(max(d), current_date()) AS hi FROM (
        SELECT order_date AS d FROM {silver}.orders
        UNION ALL SELECT footfall_date FROM {silver}.store_footfall
        UNION ALL SELECT start_date FROM {silver}.promotions
    )
),
days AS (SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS d FROM bounds)
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
    d AS date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    date_format(d, 'MMMM') AS month_name,
    date_format(d, 'yyyy-MM') AS year_month,
    day(d) AS day,
    dayofweek(d) AS day_of_week,
    date_format(d, 'EEEE') AS day_name,
    dayofweek(d) IN (1, 7) AS is_weekend
FROM days
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% dim_customer (SCD2)
build("dim_customer", f"""
SELECT
    xxhash64(customer_id, valid_from) AS customer_key,
    customer_id, customer_name, customer_category_id, delivery_city_id, credit_limit, account_opened_date,
    CASE WHEN row_number() OVER w = 1 THEN timestamp'1900-01-01' ELSE source_valid_from END AS effective_from,
    coalesce(lead(source_valid_from) OVER w, timestamp'9999-12-31') AS effective_to,
    is_current
FROM {silver}.customers
WINDOW w AS (PARTITION BY customer_id ORDER BY valid_from)
UNION ALL
SELECT -1, -1, 'Unknown', NULL, NULL, NULL, NULL, timestamp'1900-01-01', timestamp'9999-12-31', true
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% dim_stock_item (SCD2)
build("dim_stock_item", f"""
SELECT
    xxhash64(stock_item_id, valid_from) AS stock_item_key,
    stock_item_id, stock_item_name, brand, supplier_id, unit_price, recommended_retail_price,
    CASE WHEN row_number() OVER w = 1 THEN timestamp'1900-01-01' ELSE source_valid_from END AS effective_from,
    coalesce(lead(source_valid_from) OVER w, timestamp'9999-12-31') AS effective_to,
    is_current
FROM {silver}.stock_items
WINDOW w AS (PARTITION BY stock_item_id ORDER BY valid_from)
UNION ALL
SELECT -1, -1, 'Unknown', NULL, NULL, NULL, NULL, timestamp'1900-01-01', timestamp'9999-12-31', true
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% dim_store (partner footfall feed; latest name/region per store)
# Include quarantined footfall rows: a bad visitor count shouldn't drop a store from the dimension.
quarantined = f"{silver}.quarantine_store_footfall"
quarantine_rows = (
    f"UNION ALL SELECT store_id, store_name, region, footfall_date FROM {quarantined} WHERE store_id IS NOT NULL"
    if spark.catalog.tableExists(quarantined) else ""  # noqa: F821
)
build("dim_store", f"""
SELECT store_id, max_by(store_name, footfall_date) AS store_name, max_by(region, footfall_date) AS region
FROM (
    SELECT store_id, store_name, region, footfall_date FROM {silver}.store_footfall
    {quarantine_rows}
)
GROUP BY store_id
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% fact_sales - point-in-time join to the SCD2 dims on order_date
build("fact_sales", f"""
SELECT
    ol.order_line_id, o.order_id,
    CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
    coalesce(dc.customer_key, -1) AS customer_key,
    coalesce(ds.stock_item_key, -1) AS stock_item_key,
    ol.quantity, ol.unit_price, ol.tax_rate,
    CAST(ol.quantity * ol.unit_price AS DECIMAL(18,2)) AS line_amount,
    CAST(ol.quantity * ol.unit_price * ol.tax_rate / 100 AS DECIMAL(18,2)) AS tax_amount
FROM {silver}.orders o
JOIN {silver}.order_lines ol ON ol.order_id = o.order_id
LEFT JOIN dim_customer dc
       ON dc.customer_id = o.customer_id
      AND o.order_date >= CAST(dc.effective_from AS DATE) AND o.order_date < CAST(dc.effective_to AS DATE)
LEFT JOIN dim_stock_item ds
       ON ds.stock_item_id = ol.stock_item_id
      AND o.order_date >= CAST(ds.effective_from AS DATE) AND o.order_date < CAST(ds.effective_to AS DATE)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% fact_store_footfall
build("fact_store_footfall", f"""
SELECT CAST(date_format(footfall_date, 'yyyyMMdd') AS INT) AS date_key, store_id, visitors
FROM {silver}.store_footfall
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% fact_promotion - stock item version current at the promotion start
build("fact_promotion", f"""
SELECT
    p.promo_id,
    CAST(date_format(p.start_date, 'yyyyMMdd') AS INT) AS start_date_key,
    p.start_date, p.end_date,
    coalesce(ds.stock_item_key, -1) AS stock_item_key,
    p.discount_pct
FROM {silver}.promotions p
LEFT JOIN dim_stock_item ds
       ON ds.stock_item_id = p.stock_item_id
      AND p.start_date >= CAST(ds.effective_from AS DATE) AND p.start_date < CAST(ds.effective_to AS DATE)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% fact_stock_holding - current snapshot against the current stock item version
build("fact_stock_holding", f"""
SELECT
    coalesce(ds.stock_item_key, -1) AS stock_item_key,
    h.quantity_on_hand, h.reorder_level, h.target_stock_level,
    h.quantity_on_hand <= h.reorder_level AS below_reorder,
    CAST(date_format(current_date(), 'yyyyMMdd') AS INT) AS snapshot_date_key
FROM {silver}.stock_holdings h
LEFT JOIN dim_stock_item ds ON ds.stock_item_id = h.stock_item_id AND ds.is_current AND ds.stock_item_id <> -1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% fact_pos_daily (in-store sales from the real-time path, via OneLake availability) - phase 6
if spark.catalog.tableExists("rt_pos_transactions"):  # noqa: F821
    build("fact_pos_daily", """
    SELECT CAST(date_format(CAST(event_time AS DATE), 'yyyyMMdd') AS INT) AS date_key, store_id, stock_item_id,
           SUM(quantity) AS units, SUM(amount) AS revenue, COUNT(DISTINCT transaction_id) AS transactions
    FROM rt_pos_transactions
    GROUP BY 1, 2, 3
    """)
else:
    print("rt_pos_transactions shortcut not found - skipping fact_pos_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% checks + exit value for pl_master logging
unknown = {
    "fact_sales.customer_key": spark.sql("SELECT count(*) FROM fact_sales WHERE customer_key = -1").first()[0],  # noqa: F821
    "fact_sales.stock_item_key": spark.sql("SELECT count(*) FROM fact_sales WHERE stock_item_key = -1").first()[0],  # noqa: F821
    "fact_promotion.stock_item_key": spark.sql(  # noqa: F821
        "SELECT count(*) FROM fact_promotion WHERE stock_item_key = -1").first()[0],
}
print("unmatched (-1) keys:", unknown)

result = {
    "entity_id": "gold",
    "rows_written": sum(tables.values()),
    "tables": tables,
    "unknown_keys": unknown,
}
print(json.dumps(result, indent=2))
notebookutils.notebook.exit(json.dumps(result))  # noqa: F821 - provided by Fabric runtime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
