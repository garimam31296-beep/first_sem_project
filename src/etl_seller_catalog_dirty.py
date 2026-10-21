"""Clean seller catalog data, quarantine invalid rows, and upsert valid rows to Hudi."""

from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, initcap, lit, row_number, trim, when
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "ecomm_prod.yml"


def load_config():
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def main():
    config = load_config()["seller_catalog"]
    spark = (
        SparkSession.builder.appName("ETL_SELLER_CATALOG")
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
        expected = {"seller_id", "item_id", "item_name", "category", "marketplace_price", "stock_qty"}
        missing = expected.difference(df.columns)
        if missing:
            raise ValueError(f"Seller catalog input is missing columns: {sorted(missing)}")

        for name in ("seller_id", "item_id", "item_name", "category"):
            df = df.withColumn(name, trim(col(name).cast(StringType())))
        df = (
            df.withColumn("item_name", initcap(col("item_name")))
            .withColumn("category", initcap(col("category")))
            .withColumn("marketplace_price", col("marketplace_price").cast(DoubleType()))
            .withColumn("stock_qty", col("stock_qty").cast(IntegerType()))
        )

        rules = [
            (col("seller_id").isNull() | (col("seller_id") == ""), "missing_seller_id"),
            (col("item_id").isNull() | (col("item_id") == ""), "missing_item_id"),
            (col("item_name").isNull() | (col("item_name") == ""), "missing_item_name"),
            (col("category").isNull() | (col("category") == ""), "missing_category"),
            (
                col("marketplace_price").isNull() | (col("marketplace_price") < 0),
                "marketplace_price_missing_or_negative",
            ),
            (col("stock_qty").isNull() | (col("stock_qty") < 0), "stock_qty_missing_or_negative"),
        ]

        failure_reasons = []
        for condition, reason in rules:
            failure_reasons.append(when(condition, lit(reason)))
        df = df.withColumn("dq_failure_reason", concat_ws(" | ", *failure_reasons))

        # Keep one deterministic row for each seller/item key and quarantine the rest.
        duplicate_window = Window.partitionBy("seller_id", "item_id").orderBy(
            col("stock_qty").desc_nulls_last(),
            col("marketplace_price").desc_nulls_last(),
            col("item_name").asc_nulls_last(),
            col("category").asc_nulls_last(),
        )
        ranked = df.withColumn("duplicate_rank", row_number().over(duplicate_window))
        duplicate_condition = col("duplicate_rank") > 1
        ranked = ranked.withColumn(
            "dq_failure_reason",
            when(
                duplicate_condition & (col("dq_failure_reason") != ""),
                concat_ws(" | ", col("dq_failure_reason"), lit("duplicate_key_reject")),
            )
            .when(duplicate_condition, lit("duplicate_key_reject"))
            .otherwise(col("dq_failure_reason")),
        )

        failed = col("dq_failure_reason") != ""
        quarantine = (
            ranked.filter(failed)
            .withColumn("input_datasetname", lit("seller_catalog"))
            .select(
                "input_datasetname",
                col("dq_failure_reason").alias("dq_failure_reason_final"),
                *[name for name in df.columns if name != "dq_failure_reason"],
            )
        )
        clean = (
            ranked.filter((~failed) & (col("duplicate_rank") == 1))
            .drop("dq_failure_reason", "duplicate_rank")
            .withColumn("last_updated_ts", current_timestamp())
        )

        print(f"Total records: {df.count()}")
        print(f"Clean records: {clean.count()}")
        print(f"Quarantine records: {quarantine.count()}")

        quarantine_path = PROJECT_ROOT / config["quarantine_csv_path"]
        quarantine.write.mode("overwrite").option("header", "true").csv(str(quarantine_path))

        hudi_options = {
            "hoodie.table.name": "seller_catalog",
            "hoodie.datasource.write.table.name": "seller_catalog",
            "hoodie.datasource.write.recordkey.field": config["hudi_record_key"],
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
            "hoodie.datasource.write.precombine.field": config["hudi_precombine_field"],
            "hoodie.datasource.write.partitionpath.field": "category",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.metadata.enable": "false",
        }
        hudi_path = config["hudi_output_path"]
        clean.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
        print(f"Clean catalog upserted to Hudi at {hudi_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
