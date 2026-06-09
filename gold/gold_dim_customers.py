
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



silver_customers_df = spark.table(SILVER_CUSTOMERS_TABLE)

table_exists = spark.catalog.tableExists(GOLD_DIM_CUSTOMERS) 

# Write into dim_customer table
def create_new_customer_dim(dim_customers_df):(
    dim_customers_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_DIM_CUSTOMERS)
)
    
# Create surrogate key for dim_customer
if not table_exists:
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
    create_new_customer_dim(dim_customers_df)

from pyspark.sql.functions import max

def get_max_sk(current_dim_customer_df, new_rows_df):
    if new_rows_df.limit(1).count() == 0:
        return None
    
    max_sk = (
        current_dim_customer_df
        .select(
            max("customer_sk")
            .alias("max_sk")
        )
        .first()["max_sk"]
    )
    return max_sk

def append_new_versions(new_version_dim_customer_df):(
    new_version_dim_customer_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(
        GOLD_DIM_CUSTOMERS
    )
)
    
if table_exists:

    current_dim_customer_df = spark.table(GOLD_DIM_CUSTOMERS)
    new_rows_df = (
        silver_customers_df.alias("s").join(
            current_dim_customer_df.alias("c"),
            [
                col("s.customer_id") == col("c.customer_id"),
                col("s.effective_from") == col("c.effective_from")
            ],
            "leftanti"
        )
        .select(
            "customer_sk",
            "customer_id",
            "customer_city",
            "customer_state",
            "effective_from",
            "effective_to",
            "is_current"
        )
    )

    max_sk_assigned = get_max_sk(current_dim_customer_df, new_rows_df)

    if max_sk_assigned is None:
        print("No new Rows")

    else:
        new_customer_versions_df = (
            new_rows_df
            .withColumn(
                "customer_sk",
                monotonically_increasing_id()
                + max_sk_assigned
                + 1
            )
        )
        append_new_versions(new_customer_versions_df)
