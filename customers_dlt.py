# Databricks notebook source
LANDING_PATH = "/Volumes/workspace/training/customers/landing/"

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, expr, from_json
from pyspark.sql.types import StructType, StructField, StringType

@dlt.table(
    name    = "customers_bronze",
    comment = "Raw customer records ingested from landing zone via Autoloader. All columnTypes preserved. Includes _rescued_data for schema mismatches, _source_file for lineage, and _ingested_at for load timing."
)
def customers_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", LANDING_PATH + "schema/")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("pathGlobFilter", "*.csv")
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .load(LANDING_PATH)
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

contact_schema = StructType([
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True)
])

@dlt.table(
    name    = "customers_silver",
    comment = "Cleaned customer records. Types cast, bad rows filtered, contact_info JSON extracted into email and phone columns."
)
@dlt.expect_or_drop("valid_customer_no", "customer_no IS NOT NULL")
@dlt.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_drop("valid_dob", "dob IS NOT NULL")
@dlt.expect_or_drop("positive_records", "registration_date IS NOT NULL")
def customers_silver():
    return (
        dlt.read_stream("customers_bronze")
        .withColumn("dob", expr("try_to_date(dob, 'yyyy-MM-dd')"))
        .withColumn("registration_date", expr("try_to_date(registration_date, 'yyyy-MM-dd')"))
        .withColumn("pincode", expr("try_cast(pincode as int)"))
        .withColumn("contact_info", from_json(col("contact_info"), contact_schema))
        .withColumn("email", col("contact_info.email").cast("string"))
        .withColumn("phone", col("contact_info.phone").cast("string"))
        .drop("contact_info")
        
        .drop("_rescued_data", "_source_file", "_ingested_at")
    )