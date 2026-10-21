"""Clean competitor sales data, quarantine invalid rows, and upsert valid rows to Hudi."""

from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_date, lit, trim, to_date, when
from pyspark.sql.types import DoubleType, IntegerType, StringType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "ecomm_prod.yml"


def load_config():
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def apply_schema_casting(df):
    """Cast competitor sales columns to their expected Spark SQL types."""
    schema = {
        "item_id": StringType(),
        "seller_id": StringType(),
        "units_sold": IntegerType(),
        "revenue": DoubleType(),
        "marketplace_price": DoubleType(),
    }

    for name, data_type in schema.items():
        if name in df.columns:
            df = df.withColumn(name, col(name).cast(data_type))

    if "sale_date" in df.columns:
        df = df.withColumn("sale_date", to_date(col("sale_date")))

    return df


def main():
    config = load_config()["competitor_sales"]
    spark = (
        SparkSession.builder.appName("ETL_COMPETITOR_SALES")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config(
            "spark.jars.packages",
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2",
        )
        .getOrCreate()
    )

    try:
        input_path = PROJECT_ROOT / config["input_path"]
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(input_path))

        expected_columns = {
            "item_id", "seller_id", "units_sold", "revenue",
            "marketplace_price", "sale_date",
        }
        missing_columns = expected_columns.difference(df.columns)
        if missing_columns:
            raise ValueError(f"Competitor sales input is missing columns: {sorted(missing_columns)}")

        for name in df.columns:
            if dict(df.dtypes)[name] == "string":
                df = df.withColumn(name, trim(col(name)))

        df = apply_schema_casting(df)

        rules = [
            (col("item_id").isNull() | (col("item_id") == ""), "missing_item_id"),
            (col("seller_id").isNull() | (col("seller_id") == ""), "missing_seller_id"),
            (col("units_sold").isNull() | (col("units_sold") < 0), "units_sold_missing_or_negative"),
            (col("revenue").isNull() | (col("revenue") < 0), "revenue_missing_or_negative"),
            (
                col("marketplace_price").isNull() | (col("marketplace_price") < 0),
                "price_missing_or_negative",
            ),
            (
                col("sale_date").isNull() | (col("sale_date") > current_date()),
                "invalid_sale_date",
            ),
        ]

        df_dq = df.withColumn("dq_failure_reason", lit(None).cast(StringType()))
        quarantine_condition = lit(False)
        for condition, reason in rules:
            quarantine_condition = quarantine_condition | condition
            df_dq = df_dq.withColumn(
                "dq_failure_reason",
                when(condition, coalesce(col("dq_failure_reason"), lit(reason)))
                .otherwise(col("dq_failure_reason")),
            )

        clean_data = df_dq.filter(~quarantine_condition).drop("dq_failure_reason")
        quarantine_data = (
            df_dq.filter(quarantine_condition)
            .withColumn("input_datasetname", lit("competitor_sales"))
            .select(
                "input_datasetname",
                col("dq_failure_reason").alias("dq_failure_reason_final"),
                *df.columns,
            )
        )

        print(f"Total records: {df.count()}")
        print(f"Clean records: {clean_data.count()}")
        print(f"Quarantine records: {quarantine_data.count()}")

        quarantine_path = PROJECT_ROOT / config["quarantine_csv_path"]
        quarantine_data.write.mode("overwrite").option("header", "true").csv(str(quarantine_path))
        print(f"Quarantine data written to {quarantine_path}")

        hudi_options = {
            "hoodie.table.name": "competitor_sales",
            "hoodie.datasource.write.table.name": "competitor_sales",
            "hoodie.datasource.write.recordkey.field": config["hudi_record_key"],
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
            "hoodie.datasource.write.partitionpath.field": "sale_date",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.metadata.enable": "false",
        }
        precombine_field = config.get("hudi_precombine_field")
        if not precombine_field or precombine_field not in clean_data.columns:
            # The supplied CSV has sale_date but no last_updated_ts.
            precombine_field = "sale_date"
        hudi_options["hoodie.datasource.write.precombine.field"] = precombine_field

        hudi_path = config["hudi_output_path"]
        clean_data.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
        print(f"Clean data upserted to Hudi at {hudi_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
