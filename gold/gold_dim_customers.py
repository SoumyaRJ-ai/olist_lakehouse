
# Gold Layer – Customer Dimension

"""
Purpose:
- Create customer dimension from Silver SCD2 table
- Generate surrogate keys
- Preserve history
"""

from config.config_loader import load_config

config = load_config()

silver_schema = config['schemas']['silver']
silver_table = config['tables']['customers']
gold_schema = config['schemas']['gold']
gold_table = config['gold_tables']['dim_customers']



from pyspark.sql.functions import col, monotonically_increasing_id



SILVER_CUSTOMERS_TABLE = f"{silver_schema}.{silver_table}"
GOLD_DIM_CUSTOMERS     = f"{gold_schema}.{gold_table}"



GOLD_DIM_CUSTOMERS



silver_customers_df = spark.table(SILVER_CUSTOMERS_TABLE)



# Create surrogate key for dim_customer

dim_customers_df = (
    silver_customers_df
    .withColumn("customer_sk", monotonically_increasing_id())
    .select(
        col("customer_sk"),
        col("customer_id"),
        col("customer_city"),
        col("customer_state"),
        col("effective_from"),
        col("effective_to"),
        col("is_current")
    )
)



# Write into dim_customer table
(
    dim_customers_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_DIM_CUSTOMERS)
)



