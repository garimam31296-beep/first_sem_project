"""Rank in-stock catalog items by observed company and competitor sales."""

from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, first, row_number, sum as spark_sum
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "ecomm_prod.yml"


def load_config():
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)["recommendation"]


def main():
    config = load_config()
    spark = (
        SparkSession.builder.appName("CONSUMPTION_RECOMMENDATION")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config(
            "spark.jars.packages",
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2",
        )
        .getOrCreate()
    )

    try:
        catalog = (
            spark.read.format("hudi")
            .option("hoodie.metadata.enable", "false")
            .load(config["seller_catalog_hudi"])
        )
        company_sales = (
            spark.read.format("hudi")
            .option("hoodie.metadata.enable", "false")
            .load(config["company_sales_hudi"])
        )
        competitor_sales = (
            spark.read.format("hudi")
            .option("hoodie.metadata.enable", "false")
            .load(config["competitor_sales_hudi"])
        )

        catalog_by_item = catalog.groupBy("item_id").agg(
            first("item_name", ignorenulls=True).alias("item_name"),
            first("category", ignorenulls=True).alias("category"),
            avg("marketplace_price").alias("marketplace_price"),
            spark_sum("stock_qty").alias("stock_qty"),
        )
        company_by_item = company_sales.groupBy("item_id").agg(
            spark_sum("units_sold").alias("company_units_sold"),
            spark_sum("revenue").alias("company_revenue"),
        )
        competitor_by_item = competitor_sales.groupBy("item_id").agg(
            spark_sum("units_sold").alias("competitor_units_sold"),
            spark_sum("revenue").alias("competitor_revenue"),
            avg("marketplace_price").alias("competitor_avg_price"),
        )

        ranked = (
            catalog_by_item.join(company_by_item, "item_id", "left")
            .join(competitor_by_item, "item_id", "left")
            .fillna(
                0,
                subset=[
                    "stock_qty", "company_units_sold", "company_revenue",
                    "competitor_units_sold", "competitor_revenue",
                ],
            )
            .withColumn(
                "total_units_sold",
                col("company_units_sold") + col("competitor_units_sold"),
            )
            .withColumn(
                "total_revenue",
                col("company_revenue") + col("competitor_revenue"),
            )
            .filter((col("stock_qty") > 0) & (col("total_units_sold") > 0))
        )
        ranking_window = Window.orderBy(
            col("total_units_sold").desc(),
            col("total_revenue").desc(),
            col("item_id").asc(),
        )
        recommendations = (
            ranked.withColumn("recommendation_rank", row_number().over(ranking_window))
            .filter(col("recommendation_rank") <= 10)
            .select(
                "recommendation_rank", "item_id", "item_name", "category",
                "marketplace_price", "stock_qty", "company_units_sold",
                "competitor_units_sold", "total_units_sold", "total_revenue",
                "competitor_avg_price",
            )
            .orderBy("recommendation_rank")
        )

        output_path = Path(config["output_csv"])
        if not output_path.is_absolute():
            output_path = PROJECT_ROOT / output_path
        recommendations.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_path))
        print(f"Top {recommendations.count()} recommendations written to {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
