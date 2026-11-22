import os
from collections.abc import Generator
from contextlib import contextmanager

import dagster as dg
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

RAW = "s3://lake/raw/quakes/"
# Iceberg tables in the Lakekeeper catalog `lake` (Sail's default catalog)
EVENTS = "lake.quakes.events"
DAILY_REGION = "lake.quakes.daily_region"
# all_day feed: an event stops being revised once it leaves the feed (24h), 2 days covers it
RAW_LOOKBACK_DAYS = 2


@contextmanager
def sail() -> Generator[SparkSession, None, None]:
    """Spark Connect session on Sail, released after the asset body."""
    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def ensure_table(spark: SparkSession, table: str, like: DataFrame) -> None:
    """Create an empty Iceberg table with `like`'s schema, once."""
    _, ns, name = table.split(".")
    like.createOrReplaceTempView("_schema_src")
    # explicit LOCATION: by default Sail uses <namespace location>/<table>, and Lakekeeper's flat
    # layout makes every namespace location the warehouse root, so ns1.t and ns2.t would collide
    # (docs/sail-findings.md, problem 3)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING iceberg "
        f"LOCATION 's3://lake/warehouse/{ns}/{name}' "
        "AS SELECT * FROM _schema_src WHERE false"
    )


# Produced continuously by fetcher.
raw_quakes = dg.AssetSpec(
    "raw_quakes", group_name="quakes", description=f"USGS events as NDJSON at {RAW}"
)


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description=f"Iceberg {EVENTS}: one row per event, latest revision. Merged incrementally.",
)
def quakes_events(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with sail() as spark:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.quakes")
        raw = spark.read.json(RAW).where(
            F.col("dt") >= F.date_sub(F.current_date(), RAW_LOOKBACK_DAYS)
        )
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
        ensure_table(spark, EVENTS, events)
        events.createOrReplaceTempView("batch")
        # idempotent: re-running with the same raw data changes nothing
        spark.sql(f"""
            MERGE INTO {EVENTS} AS t USING batch AS b ON t.id = b.id
            WHEN MATCHED AND b.updated_time > t.updated_time THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        # Every MERGE adds data files, and in cluster mode Sail scans the MERGE target with one task
        # per file, all at once: past (max workers x task slots) files the run fails. Compact back
        # to a single file after each merge. ponytail: rewrites the whole table every run, fine at
        # this size; use binpack thresholds or a separate maintenance schedule when it grows.
        compacted = spark.sql(f"""
            CALL lake.system.rewrite_data_files(
              table => '{EVENTS.removeprefix("lake.")}', options => map('rewrite-all', 'true'))
        """).collect()[0]
        batch_rows = events.count()
        rows = spark.table(EVENTS).count()
    return dg.MaterializeResult(
        metadata={
            "table": EVENTS,
            "rows": rows,
            "batch_rows": batch_rows,
            "files_compacted": compacted["rewritten_data_files_count"],
        }
    )


@dg.asset(
    deps=[quakes_events],
    group_name="quakes",
    description=f"Iceberg {DAILY_REGION}: daily event stats per region. Full recompute.",
)
def quakes_daily_region(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with sail() as spark:
        agg = (
            spark.table(EVENTS)
            .groupBy(F.to_date("event_time").alias("day"), "region")
            .agg(
                F.count("*").alias("events"),
                F.round(F.avg("mag"), 2).alias("avg_mag"),
                F.max("mag").alias("max_mag"),
                F.round(F.avg("depth_km"), 1).alias("avg_depth_km"),
            )
        )
        ensure_table(spark, DAILY_REGION, agg)
        agg.createOrReplaceTempView("agg")
        # one atomic snapshot: readers see the old or the new table, never half of it
        spark.sql(f"INSERT OVERWRITE {DAILY_REGION} SELECT * FROM agg")

        out = spark.table(DAILY_REGION)
        top = out.orderBy(F.desc("events")).limit(10).toPandas()
        # global agg always yields exactly one row; coalesce covers an empty table
        stats = out.agg(
            F.count("*").alias("rows"),
            F.coalesce(F.sum("events"), F.lit(0)).alias("events"),
        ).collect()[0]
    return dg.MaterializeResult(
        metadata={
            "table": DAILY_REGION,
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
            name="quakes_15min",
            target=[quakes_events, quakes_daily_region],
            cron_schedule="5-59/15 * * * *",  # :05 :20 :35 :50
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )
    ],
)
