# Bronze Layer – Products

"""
Purpose:
- Ingest raw Olist products CSV data into the Bronze layer
- Append-only ingestion
- Preserve raw structure with minimal type casting
- Add ingestion metadata
"""

from config.config_loader import load_config

config = load_config()

raw_base_path = config['paths']['raw_base_path']
bronze_schema = config['schemas']['bronze']
bronze_table_products = config['tables']['products']



# Imports

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, IntegerType



RAW_BASE_PATH = f"{raw_base_path}/bronze/olist_products_dataset.csv"
BRONZE_TABLE  = f"{bronze_schema}.{bronze_table_products}"



products_raw_df = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", False)
    .load(RAW_BASE_PATH)
)



# Select and cast schema (minimal, explicit)

products_bronze_df = (
    products_raw_df
    .select(
      col("product_id").cast(StringType()),
      col("product_category_name").cast(StringType()),
      col("product_name_lenght").cast(IntegerType()),
      col("product_description_lenght").cast(IntegerType()),
      col("product_photos_qty").cast(IntegerType()),
      col("product_weight_g").cast(IntegerType()),
      col("product_length_cm").cast(IntegerType()),
      col("product_height_cm").cast(IntegerType()),
      col("product_width_cm").cast(IntegerType())
    )
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)



# Write to Bronze table (append-only)

(
    products_bronze_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE_TABLE)
)



# Basic validation

spark.table(BRONZE_TABLE).count()




## Bronze Products Ingestion – Completed
"""
- Append-only ingestion
- Raw product attributes preserved
- No business logic applied
"""