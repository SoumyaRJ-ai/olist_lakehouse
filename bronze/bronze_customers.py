
# Bronze Layer – Customers
"""
**Purpose**
- Ingest raw Olist customers CSV data into the Bronze layer
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
bronze_table_customers = config['tables']['customers']



# Imports

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType



RAW_BASE_PATH = f"{raw_base_path}/customers/olist_customers_dataset.csv"
BRONZE_TABLE  = f"{bronze_schema}.{bronze_table_customers}"



customers_raw_df = (
    spark.read
    .format("csv")
    .option("inferSchema", False)
    .option("header", True)
    .load(RAW_BASE_PATH)
)



# Select and cast schema (minimal, explicit)

customers_bronze_df = (
    customers_raw_df
        .select(
            col("customer_id").cast(StringType()),
            col("customer_unique_id").cast(StringType()),
            col("customer_zip_code_prefix").cast(StringType()),
            col("customer_city").cast(StringType()),
            col("customer_state").cast(StringType())
        )
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
)



# Write to Bronze table (append-only)

(
    customers_bronze_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
)


spark.table(BRONZE_TABLE).count()


"""
**What we did**
- Ingested raw customers data from Databricks Volumes
- Appended to a Delta Bronze table
- Added ingestion metadata

**What we intentionally did NOT do**
- Clean city/state values
- Deduplicate customers
- Apply business rules

These steps are handled in the Silver layer.
"""