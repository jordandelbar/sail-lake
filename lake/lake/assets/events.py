import dagster as dg
from pydantic import Field
from pyspark.sql import Window
from pyspark.sql import functions as F

from lake.lakehouse import Lakehouse
from lake.usgs import RAW, read_raw, region_of

EVENTS = "quakes.events"

raw_quakes = dg.AssetSpec(
    "raw_quakes",
    group_name="quakes",
    description=f"USGS events as NDJSON at {RAW}, landed continuously by the ingest pipeline",
)


class EventsConfig(dg.Config):
    raw_lookback_days: int = Field(
        default=2,
        description="Days of raw/ (by landing date) merged per run. The live feed stops revising "
        "an event once it leaves the 24h feed, so 2 days covers every revision.",
    )


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description="Iceberg quakes.events: one row per event, latest revision. Merged incrementally.",
)
def quakes_events(config: EventsConfig, lakehouse: Lakehouse) -> dg.MaterializeResult:
    table = lakehouse.table(EVENTS)
    with lakehouse.session() as spark:
        raw = read_raw(spark).where(
            F.col("dt") >= F.date_sub(F.current_date(), config.raw_lookback_days)
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
            region_of("place").alias("region"),
            F.col("type").alias("event_type"),
            (F.col("tsunami") == 1).alias("tsunami"),
            "lon",
            "lat",
            F.col("depth").alias("depth_km"),
        )
        lakehouse.ensure_table(spark, EVENTS, events)
        events.createOrReplaceTempView("batch")
        # idempotent: re-running with the same raw data changes nothing
        spark.sql(f"""
            MERGE INTO {table} AS t USING batch AS b ON t.id = b.id
            WHEN MATCHED AND b.updated_time > t.updated_time THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        compacted = lakehouse.compact(spark, EVENTS)
        batch_rows = events.count()
        rows = spark.table(table).count()
    return dg.MaterializeResult(
        metadata={
            "table": table,
            "rows": rows,
            "batch_rows": batch_rows,
            "files_compacted": compacted,
        }
    )
