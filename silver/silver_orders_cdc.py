# Silver Layer – Orders (CDC-style)

"""
**Purpose**
- Clean and standardize orders from Bronze
- Handle updates to orders (status, timestamps)
- Maintain one latest version per order

**Why CDC here?**
- Orders can change status over time
- We only care about the *latest* state of an order

**Design Choice**
- Use MERGE for idempotent upserts
- No history tracking (SCD Type 1 behavior)

"""

from config.config_loader import load_config

config = load_config()

bronze_schema = config['schemas']['bronze']
silver_schema = config['schemas']['silver']
table_orders = config['tables']['orders']




from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp
from pyspark.sql.types import StringType, TimestampType
from delta.tables import DeltaTable




# Configuration

BRONZE_TABLE = f"{bronze_schema}.{table_orders}"
SILVER_TABLE = f"{silver_schema}.{table_orders}"



# Read orders from Bronze

bronze_orders_df = spark.table(BRONZE_TABLE)



# Standardize and clean orders cast and trim and add new column

orders_clean_df = (
    bronze_orders_df
        .select(
            col("order_id").cast(StringType()),
            col("customer_id").cast(StringType()),
            trim(col("order_status")).alias("order_status"),
            col("order_purchase_timestamp").cast(TimestampType()),
            col("order_approved_at").cast(TimestampType()),
            col("order_delivered_carrier_date").cast(TimestampType()),
            col("order_delivered_customer_date").cast(TimestampType()),
            col("order_estimated_delivery_date").cast(TimestampType())
        )
        .withColumn("last_updated_ts", current_timestamp())
)



table_exists = spark.catalog.tableExists(SILVER_TABLE)



if not table_exists:
    (
        orders_clean_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )



# CDC-style MERGE (idempotent upsert)

if table_exists:
    delta_table = DeltaTable.forName(spark, SILVER_TABLE)

    (
        delta_table.alias("target")
        .merge(
            orders_clean_df.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdate(
            condition="""
                target.order_status <> source.order_status
                OR target.order_delivered_customer_date <> source.order_delivered_customer_date
                OR target.order_delivered_carrier_date <> source.order_delivered_carrier_date
            """,
            set={
                "order_status": col("source.order_status"),
                "order_purchase_timestamp": col("source.order_purchase_timestamp"),
                "order_approved_at": col("source.order_approved_at"),
                "order_delivered_carrier_date": col("source.order_delivered_carrier_date"),
                "order_delivered_customer_date": col("source.order_delivered_customer_date"),
                "order_estimated_delivery_date": col("source.order_estimated_delivery_date"),
                "last_updated_ts": col("source.last_updated_ts")
            }
        )
        .whenNotMatchedInsert(
            values={
                "order_id": col("source.order_id"),
                "customer_id": col("source.customer_id"),
                "order_status": col("source.order_status"),
                "order_purchase_timestamp": col("source.order_purchase_timestamp"),
                "order_approved_at": col("source.order_approved_at"),
                "order_delivered_carrier_date": col("source.order_delivered_carrier_date"),
                "order_delivered_customer_date": col("source.order_delivered_customer_date"),
                "order_estimated_delivery_date": col("source.order_estimated_delivery_date"),
                "last_updated_ts": col("source.last_updated_ts")
            }
        )
        .execute()
    )




spark.table(SILVER_TABLE) \
     .groupBy("order_id") \
     .count() \
     .where(col("count") > 1).display()



## Silver Orders (CDC) - Completed
"""
**What we did**
- Standardized orders data
- Used MERGE for CDC-style upserts
- Ensured idempotent processing

**What we intentionally did NOT do**
- Track historical order versions
- Streaming or watermarking
- Complex conflict resolution
"""