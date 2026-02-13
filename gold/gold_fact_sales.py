# Gold Layer – Fact Sales

"""
Purpose:
- Create an analytics-ready fact table at order-item grain
- Join orders, customers, and order items
- Enforce basic data validity and referential integrity
- MERGE-based incremental logic.
- Add business key
- Grain = order_id + order_item_id
"""

from config.config_loader import load_config

config = load_config()

bronze_schema = config['schemas']['bronze']
bronze_table_order_items = config['tables']['order_items']

silver_schema = config['schemas']['silver']
silver_table_orders = config['tables']['orders']
silver_table_customers = config['tables']['customers']

gold_schema = config['schemas']['gold']
gold_table_fact = config['gold_tables']['fact_sales']
gold_table_dim_customers = config['gold_tables']['dim_customers']
gold_table_dim_products = config['gold_tables']['dim_products']



from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable



# Configuration

SILVER_ORDERS_TABLE    = f"{silver_schema}.{silver_table_orders}"
SILVER_CUSTOMERS_TABLE = f"{silver_schema}.{silver_table_customers}"
BRONZE_ORDER_ITEMS     = f"{bronze_schema}.{bronze_table_order_items}"
GOLD_FACT_TABLE        = f"{gold_schema}.{gold_table_fact}"
GOLD_DIM_CUSTOMERS     = f"{gold_schema}.{gold_table_dim_customers}"
GOLD_DIM_PRODUCTS      = f"{gold_schema}.{gold_table_dim_products}"



# Read source tables

orders_df = spark.table(SILVER_ORDERS_TABLE)
customers_dim_df = (
    spark.table(GOLD_DIM_CUSTOMERS)
         .where(col("is_current") == True)
)
products_dim_df = spark.table(GOLD_DIM_PRODUCTS)
order_items_df = spark.table(BRONZE_ORDER_ITEMS)



# Defensive filtering on order_items (minimal, high-signal)
# order_id, product_id, price should not be NULL, price should > 0, change price to DoubleType

order_items_cleaned_df = (
    order_items_df
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("price").isNotNull())
    .filter(col("price") > 0)
    .withColumn("price", col("price").cast(DoubleType()))
)



# Join with customers (inner join ensures valid customer mapping)

fact_sales_df = (
    order_items_cleaned_df
        .join(orders_df, on="order_id", how="inner")
        .join(customers_dim_df, on="customer_id", how="inner")
        .join(products_dim_df, on="product_id", how="inner")
        .select(
            col("order_id"),
            col("order_item_id"),
            col("customer_sk"),
            col("product_sk"),
            col("order_status"),
            col("order_purchase_timestamp").alias("order_ts"),
            col("price"),
            col("freight_value"),
            current_timestamp().alias("ingestion_ts")
        )
        .dropDuplicates(["order_id", "order_item_id"])
)



table_exists = spark.catalog.tableExists(GOLD_FACT_TABLE)



# Write Gold fact table (overwrite for simplicity)

if not table_exists:
    (
        fact_sales_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(GOLD_FACT_TABLE)
    )



# Basic validation

spark.table(GOLD_FACT_TABLE).count()




### Incremental logic starts



if table_exists:
  delta_table = DeltaTable.forName(spark, GOLD_FACT_TABLE)

  (
    delta_table.alias("target")
    .merge(
      fact_sales_df.alias("source"),
      condition="""
        target.order_id = source.order_id
        AND target.order_item_id = source.order_item_id
      """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
  )

  # When conditions match, Update all
  # When conditions don't match i.e. new comers, Insert all



# Sanity Validation

spark.table(GOLD_FACT_TABLE) \
     .groupBy("order_id", "order_item_id") \
     .count() \
     .where(col("count") > 1).show()




### Capture rejected order_items



GOLD_REJECT_TABLE = "olist.gold.fact_sales_rejects"



# Identify rejected order items

rejected_order_items_df = (
    order_items_df
        .where(
            col("order_id").isNull() |
            col("product_id").isNull() |
            col("price").isNull() |
            (col("price") < 0)
        )
)



(
    rejected_order_items_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_REJECT_TABLE)
)

# Good data → fact_sales
# Bad data → fact_sales_rejects



