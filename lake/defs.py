import os
from collections.abc import Iterator
from contextlib import contextmanager

import dagster as dg
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = "s3://lake/raw/quakes/"
EVENTS = "s3://lake/clean/quakes_events/"
DAILY_REGION = "s3://lake/agg/quakes_daily_region/"


@contextmanager
def sail() -> Iterator[SparkSession]:
    """Spark Connect session on Sail, released after the asset body."""
    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


# Produced continuously by fetcher.
raw_quakes = dg.AssetSpec(
    "raw_quakes", group_name="quakes", description=f"USGS events as NDJSON at {RAW}"
)


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description=f"One row per event (latest revision), Parquet at {EVENTS}",
)
def quakes_events(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with sail() as spark:
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
        events = latest.select(
            "id",
            F.timestamp_millis("time").alias("event_time"),
            F.timestamp_millis("updated").alias("updated_time"),
            "mag",
            F.col("magType").alias("mag_type"),
            "place",
            # "10 km NW of X, Alaska" -> "Alaska"
            F.trim(
                F.element_at(F.split(F.coalesce("place", F.lit("unknown")), ","), -1)
            ).alias("region"),
            F.col("type").alias("event_type"),
            (F.col("tsunami") == 1).alias("tsunami"),
            "lon",
            "lat",
            F.col("depth").alias("depth_km"),
        )
        # ponytail: plain Parquet overwrite, readers can see a half-written table. Delta when that matters.
        events.write.mode("overwrite").parquet(EVENTS)
        rows = spark.read.parquet(EVENTS).count()
    return dg.MaterializeResult(metadata={"path": EVENTS, "rows": rows})


@dg.asset(
    deps=[quakes_events],
    group_name="quakes",
    description=f"Daily event stats per region, Parquet at {DAILY_REGION}",
)
def quakes_daily_region(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with sail() as spark:
        agg = (
            spark.read.parquet(EVENTS)
            .groupBy(F.to_date("event_time").alias("day"), "region")
            .agg(
                F.count("*").alias("events"),
                F.round(F.avg("mag"), 2).alias("avg_mag"),
                F.max("mag").alias("max_mag"),
                F.round(F.avg("depth_km"), 1).alias("avg_depth_km"),
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


defs = dg.Definitions(
    assets=[raw_quakes, quakes_events, quakes_daily_region],
    schedules=[
        dg.ScheduleDefinition(
            name="quakes_hourly",
            target=[quakes_events, quakes_daily_region],
            cron_schedule="5 * * * *",
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )
    ],
)
