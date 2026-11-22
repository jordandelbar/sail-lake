from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lake.quakes.raw import region_of


def events(raw: DataFrame) -> DataFrame:
    """One row per event: its latest version (highest `updated`), whatever the source."""
    latest = (
        raw.withColumn(
            "rn",
            F.row_number().over(Window.partitionBy("id").orderBy(F.desc("updated"))),
        )
        .filter("rn = 1")
        .drop("rn")
    )
    return latest.select(
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


def merge_events(table: str, batch: DataFrame) -> None:
    """Upsert `batch` (output of `events`) into `table`: an existing event is replaced only by a
    newer version. Idempotent: merging the same batch again changes nothing."""
    batch.createOrReplaceTempView("_events_batch")
    batch.sparkSession.sql(f"""
        MERGE INTO {table} AS t USING _events_batch AS b ON t.id = b.id
        WHEN MATCHED AND b.updated_time > t.updated_time THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def daily_region(events: DataFrame) -> DataFrame:
    """Daily event count, average and max magnitude, average depth per region."""
    return events.groupBy(F.to_date("event_time").alias("day"), "region").agg(
        F.count("*").alias("events"),
        F.round(F.avg("mag"), 2).alias("avg_mag"),
        F.max("mag").alias("max_mag"),
        F.round(F.avg("depth_km"), 1).alias("avg_depth_km"),
    )
