import os

import dagster as dg
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = "s3://lake/raw/quakes/"
DAILY_REGION = "s3://lake/agg/quakes_daily_region/"

# Produced continuously by fetcher.
raw_quakes = dg.AssetSpec(
    "raw_quakes", group_name="quakes", description=f"USGS events as NDJSON at {RAW}"
)


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description=f"Daily event stats per region, Parquet at {DAILY_REGION}",
)
def quakes_daily_region(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    try:
        raw = spark.read.json(RAW)
        # USGS revises events and the feed re-emits them with a newer `updated`; keep the last one
        latest = (
            raw.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("id").orderBy(F.desc("updated"))
                ),
            )
            .filter("rn = 1")
            .drop("rn")
        )
        agg = (
            latest.withColumn("day", F.to_date(F.from_unixtime(F.col("time") / 1000)))
            .withColumn(
                "region",
                F.trim(
                    F.element_at(
                        F.split(F.coalesce("place", F.lit("unknown")), ","), -1
                    )
                ),
            )
            .groupBy("day", "region")
            .agg(
                F.count("*").alias("events"),
                F.round(F.avg("mag"), 2).alias("avg_mag"),
                F.max("mag").alias("max_mag"),
                F.round(F.avg("depth"), 1).alias("avg_depth_km"),
            )
        )
        agg.write.mode("overwrite").parquet(DAILY_REGION)

        out = spark.read.parquet(DAILY_REGION)
        top = out.orderBy(F.desc("events")).limit(10).toPandas()
        # global agg always yields exactly one row; coalesce covers an empty table
        stats = out.agg(
            F.count("*").alias("rows"),
            F.coalesce(F.sum("events"), F.lit(0)).alias("events"),
        ).collect()[0]
        return dg.MaterializeResult(
            metadata={
                "path": DAILY_REGION,
                "rows": stats["rows"],
                "events": stats["events"],
                "top_regions": dg.MetadataValue.md(
                    f"```\n{top.to_string(index=False)}\n```"
                ),
            }
        )
    finally:
        spark.stop()


defs = dg.Definitions(
    assets=[raw_quakes, quakes_daily_region],
    schedules=[
        dg.ScheduleDefinition(
            name="quakes_hourly",
            target=quakes_daily_region,
            cron_schedule="5 * * * *",
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )
    ],
)
