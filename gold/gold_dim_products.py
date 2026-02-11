# Gold Layer – Product Dimension

"""
Purpose:
- Create an analytics-ready product dimension
- Fix source column misspellings
- Generate surrogate keys
- Keep latest attributes (SCD Type 1)
"""

from config.config_loader import load_config

config = load_config()

bronze_schema = config['schemas']['bronze']
bronze_table = config['tables']['products']
gold_schema = config['schemas']['gold']
gold_table = config['gold_tables']['dim_products']



from pyspark.sql.functions import col, trim, monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType



BRONZE_PRODUCTS_TABLE = f"{bronze_schema}.{bronze_table}"
GOLD_DIM_PRODUCTS     = f"{gold_schema}.{gold_table}"



bronze_products_df = spark.table(BRONZE_PRODUCTS_TABLE)



# Select, cast, and FIX misspelled column names (Gold responsibility)

products_clean_df = (
    bronze_products_df
        .select(
            col("product_id").cast(StringType()),
            trim(col("product_category_name")).alias("product_category_name"),
            col("product_name_lenght").cast(IntegerType()).alias("product_name_length"),
            col("product_description_lenght").cast(IntegerType()).alias("product_description_length"),
            col("product_photos_qty").cast(IntegerType()),
            col("product_weight_g").cast(IntegerType()),
            col("product_length_cm").cast(IntegerType()),
            col("product_height_cm").cast(IntegerType()),
            col("product_width_cm").cast(IntegerType())
        )
)



# Generate surrogate key

dim_products_df = (
    products_clean_df
    .withColumn("product_sk", monotonically_increasing_id())
    .select(
        col("product_sk"),
        col("product_id"),
        col("product_category_name"),
        col("product_name_length"),
        col("product_description_length"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    )
)



# Write Gold dimension (SCD Type 1 – overwrite)

(
    dim_products_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_DIM_PRODUCTS)
)



# Basic validation

spark.table(GOLD_DIM_PRODUCTS).count()



## Gold Product Dimension – Completed
"""
- Source misspellings corrected
- Surrogate keys generated
- Analytics-ready schema
"""