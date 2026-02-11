# Bronze Layer – Orders

"""
**Purpose**
- Ingest raw Olist orders CSV data into the Bronze layer
- Append-only ingestion
- Minimal transformation
- Add ingestion metadata

**Design Principles**
- No deduplication
- No data quality validation
- No MERGE logic
- Bronze = raw, immutable data

"""

from config.config_loader import load_config

config = load_config()

raw_base_path = config['paths']['raw_base_path']
bronze_schema = config['schemas']['bronze']
bronze_table_orders = config['tables']['orders']



# Imports

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, TimestampType 



RAW_BASE_PATH = f"{raw_base_path}/orders/olist_orders_dataset.csv"
BRONZE_TABLE  = f"{bronze_schema}.{bronze_table_orders}"



orders_raw_df = (
    spark.read
    .format("csv")
    .option("inferSchema", False)
    .option("header", True)
    .load(RAW_BASE_PATH)
)



# Select and cast schema (minimal, explicit)
# Casting ≠ cleansing.
# We are only ensuring correct types, not fixing data issues.

orders_bronze_df = (
    orders_raw_df.select(
        col("order_id").cast(StringType()),
        col("customer_id").cast(StringType()),
        col("order_status").cast(StringType()),
        col("order_purchase_timestamp").cast(TimestampType()),
        col("order_approved_at").cast(TimestampType()),
        col("order_delivered_carrier_date").cast(TimestampType()),
        col("order_delivered_customer_date").cast(TimestampType()),
        col("order_estimated_delivery_date").cast(TimestampType())
    ).withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)





### Since the project runs on Unity Catalog, 
# I use _metadata.file_path instead of input_file_name() to capture source file lineage.



# Write to Bronze table (append-only)

(
  orders_bronze_df
  .write 
  .format("delta")
  .mode("append")
  .saveAsTable(BRONZE_TABLE)
)



# Simple validation (row count only – optional, informational)

spark.table(BRONZE_TABLE).count()




## Bronze Orders Ingestion – Completed

"""
**What we achieved**
- Raw orders ingested from Databricks Volumes
- Append-only Delta table
- Ingestion metadata added

**What is intentionally NOT done here**
- Deduplication
- CDC logic
- Data quality checks
- Business rule validation
"""