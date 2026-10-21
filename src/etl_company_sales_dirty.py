"""Clean company sales data, quarantine invalid rows, and upsert valid rows to Hudi."""

from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_date, lit, trim, to_date, when
from pyspark.sql.types import DoubleType, IntegerType, StringType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "ecomm_prod.yml"


def load_config():
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def main():
    config = load_config()["company_sales"]
    spark = (
        SparkSession.builder.appName("ETL_COMPANY_SALES")
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
        expected = {"item_id", "units_sold", "revenue", "sale_date"}
        missing = expected.difference(df.columns)
        if missing:
            raise ValueError(f"Company sales input is missing columns: {sorted(missing)}")

        df = (
            df.withColumn("item_id", trim(col("item_id").cast(StringType())))
            .withColumn("units_sold", col("units_sold").cast(IntegerType()))
            .withColumn("revenue", col("revenue").cast(DoubleType()))
            .withColumn("sale_date", to_date(col("sale_date")))
        )

        rules = [
            (col("item_id").isNull() | (col("item_id") == ""), "missing_item_id"),
            (col("units_sold").isNull() | (col("units_sold") < 0), "units_sold_missing_or_negative"),
            (col("revenue").isNull() | (col("revenue") < 0), "revenue_missing_or_negative"),
            (
                col("sale_date").isNull() | (col("sale_date") > current_date()),
                "invalid_sale_date",
            ),
        ]
        failure_reasons = []
        quarantine_condition = lit(False)
        for condition, reason in rules:
            quarantine_condition = quarantine_condition | condition
            failure_reasons.append(when(condition, lit(reason)))
        failure_reason = concat_ws(" | ", *failure_reasons)
        tagged = df.withColumn("dq_failure_reason", failure_reason)

        quarantine = (
            tagged.filter(quarantine_condition)
            .withColumn("input_datasetname", lit("company_sales"))
            .select("input_datasetname", col("dq_failure_reason").alias("dq_failure_reason_final"), *df.columns)
        )
        clean = tagged.filter(~quarantine_condition).drop("dq_failure_reason")

        print(f"Total records: {df.count()}")
        print(f"Clean records: {clean.count()}")
        print(f"Quarantine records: {quarantine.count()}")

        quarantine_path = PROJECT_ROOT / config["quarantine_csv_path"]
        quarantine.write.mode("overwrite").option("header", "true").csv(str(quarantine_path))

        hudi_options = {
            "hoodie.table.name": "company_sales",
            "hoodie.datasource.write.table.name": "company_sales",
            "hoodie.datasource.write.recordkey.field": config["hudi_record_key"],
            "hoodie.datasource.write.precombine.field": config["hudi_precombine_field"],
            "hoodie.datasource.write.partitionpath.field": "sale_date",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.metadata.enable": "false",
        }
        hudi_path = config["hudi_output_path"]
        clean.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
        print(f"Clean company sales upserted to Hudi at {hudi_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
