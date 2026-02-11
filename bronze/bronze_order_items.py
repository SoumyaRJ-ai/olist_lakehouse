# Bronze Layer – Order Item

"""

**Purpose**
- Ingest raw Olist order items CSV data into the Bronze layer
- Append-only ingestion
- Minimal transformation (type casting only)
- Add ingestion metadata

**Design Principles**
- No deduplication
- No data quality checks
- No MERGE logic
- Bronze = raw, immutable data

"""

from config.config_loader import load_config

config = load_config()

raw_base_path = config['paths']['raw_base_path']
bronze_schema = config['schemas']['bronze']
bronze_table_order_items = config['tables']['order_items']



# Imports

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType



# Configuration (Databricks Volumes – storage-agnostic via config)

RAW_BASE_PATH = f"{raw_base_path}/order_items/olist_order_items_dataset.csv"
BRONZE_TABLE  = f"{bronze_schema}.{bronze_table_order_items}"


order_items_raw_df = (
    spark.read
    .format("csv")
    .option("inferSchema", False)
    .option("header", True)
    .load(RAW_BASE_PATH)
)


# Select and cast schema (minimal, explicit)

order_items_bronze_df = (
    order_items_raw_df
        .select(
            col("order_id").cast(StringType()),
            col("order_item_id").cast(IntegerType()),
            col("product_id").cast(StringType()),
            col("seller_id").cast(StringType()),
            col("shipping_limit_date").cast(TimestampType()),
            col("price").cast(DoubleType()),
            col("freight_value").cast(DoubleType())
        )
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
)


# Write to Bronze table (append-only)

(
    order_items_bronze_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
)


# Simple validation (informational only)

spark.table(BRONZE_TABLE).count()


## Bronze Order Items Ingestion – Completed
"""
**What we did**
- Ingested raw order items data from Databricks Volumes
- Appended data to a Delta Bronze table
- Added ingestion metadata for lineage

**What we intentionally did NOT do**
- Deduplication
- Key enforcement
- Business validation

These concerns are handled in the Silver layer.
"""