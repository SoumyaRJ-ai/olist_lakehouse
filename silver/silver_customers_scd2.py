# Silver Layer – Customers (SCD Type 2)

"""
**Purpose**
- Standardize customer data from Bronze
- Track historical changes using SCD Type 2
- Maintain one current record per customer

**Why SCD Type 2?**
- Customer attributes (city/state) can change over time
- Analytics may need historical context

**Two-step SCD Type 2**
1. Expire changed records
2. Insert new versions

**Design Principles**
- Idempotent processing using MERGE
- Clear business key
- No complex late-arriving logic

"""

from config.config_loader import load_config

config = load_config()

bronze_schema = config['schemas']['bronze']
silver_schema = config['schemas']['silver']
table_customers = config['tables']['customers']




# Imports

from pyspark.sql.functions import col, trim, current_timestamp, lit, sum
from pyspark.sql.types import StringType, TimestampType
from delta.tables import DeltaTable



# Configuration

BRONZE_TABLE = f"{bronze_schema}.{table_customers}"
SILVER_TABLE = f"{silver_schema}.{table_customers}"

FAR_FUTURE_DATE = "9999-12-31"

# “I use a far-future date to represent the current active record.”



# Read customers from Bronze

bronze_customers_df = spark.table(BRONZE_TABLE)



# Standardize customer attributes

incoming_clean_df = (
    bronze_customers_df
        .select(
            col("customer_id").cast(StringType()),
            trim(col("customer_city")).alias("customer_city"),
            trim(col("customer_state")).alias("customer_state")
        )
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit(FAR_FUTURE_DATE).cast(TimestampType()))
        .withColumn("is_current", lit(True))
)

# The new column addition here is part of SCD Type - 2 semantics



# Check if Silver table exists

table_exists = spark.catalog.tableExists(SILVER_TABLE)



# Initial load (first run)

if not table_exists:
    (
        incoming_clean_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_TABLE)
    )

# On the first run, I load the entire dataset.
# Subsequent runs use MERGE for idempotency.



# Incremental load with SCD Type 2 logic (2 parts)
# Part 1 - If changed data comes then EXPIRE 

if table_exists:
    delta_table = DeltaTable.forName(spark, SILVER_TABLE)

    # join condition on business key
    merge_condition= """
        target.customer_id = source.customer_id
        AND target.is_current = true
    """

    (
        delta_table.alias("target")
        .merge(
            incoming_clean_df.alias("source"),
            merge_condition
        )
        # When attributes change, expire old one
        .whenMatchedUpdate(
            condition="""
                target.customer_city <> source.customer_city
                OR target.customer_state <> source.customer_state
            """,
            set={
                "effective_to": current_timestamp(),
                "is_current": lit(False)
            }
        )
        .execute()
    )

# Only expires old versions
# Does not insert anything
# History is preserved



# Find customers that need new rows (new OR changed)
# Part - 2 this is the 2nd part where we insert other part

current_silver_df = spark.table(SILVER_TABLE).where("is_current = true")

new_version_df = (
    incoming_clean_df.alias("source")
    .join(
        current_silver_df.alias("target"),
        on="customer_id",
        how="left"
    )
    .where(
        col("target.customer_id").isNull() |
        (col("source.customer_city") != col("target.customer_city")) |
        (col("source.customer_state") != col("target.customer_state"))
    )
    .select("source.*")
)

# For a new row the customer_id will be NULL for a left join
# For an updated row the state or city needs to be updated, so we are INSERTing a new row



# Add the newly found data or updated ones in the SILVER table
(
    new_version_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(SILVER_TABLE)
)



(
    spark.table(SILVER_TABLE)
         .where(col("is_current") == True)
         .groupBy("customer_id")
         .count()
         .where(col("count") > 1)
).display()

# Should not return anything as we have not run any updates




# Silver completed for customers
"""
**What we did**
- We read the Bronze data first
- Then we cleaned it against whitespaces and added columns required for SCD Type-2
- We checked if the SILVER table already exists or not
- If it does not exist then create, If it is then UPDATE
- We utilized SCD Type-2 for Updates and Insert
- If the merge condition is satisfied then we EXPIRE the current data first
- Then INSERT the new ones or the changed ones
"""