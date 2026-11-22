from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lake.quakes.raw import live, region_of


def revisions(raw: DataFrame) -> DataFrame:
    """Per live event, the versions this pipeline captured (backfill rows are ignored: they only
    hold final versions). Earliest and latest are the earliest and latest *captured* versions,
    not USGS's first publication or final answer; changes are net changes between those two.
    """
    rows = live(raw)
    oldest = Window.partitionBy("id").orderBy("updated")
    newest = Window.partitionBy("id").orderBy(F.desc("updated"))
    earliest = (
        rows.withColumn("rn", F.row_number().over(oldest))
        .where("rn = 1")
        .select(
            "id",
            "time",
            F.col("updated").alias("earliest_updated"),
            F.col("mag").alias("earliest_mag"),
            F.col("type").alias("earliest_type"),
        )
    )
    latest = (
        rows.withColumn("rn", F.row_number().over(newest))
        .where("rn = 1")
        .select(
            "id",
            F.col("updated").alias("latest_updated"),
            F.col("mag").alias("latest_mag"),
            F.col("type").alias("latest_type"),
            region_of("place").alias("region"),
        )
    )
    # The same version can sit in raw/ more than once (re-published after a NATS reset).
    # observed is null for rows captured before the fetcher recorded it; if any row of an event
    # lacks it, its first observation may predate the recorded ones, so it is unknown.
    per_event = rows.groupBy("id").agg(
        F.count_distinct("updated").alias("versions_captured"),
        F.when(F.count("observed") == F.count("*"), F.min("observed")).alias(
            "first_observed"
        ),
    )
    return (
        earliest.join(latest, "id")
        .join(per_event, "id")
        .select(
            "id",
            F.timestamp_millis("time").alias("event_time"),
            "region",
            "versions_captured",
            F.round((F.col("first_observed") - F.col("time")) / 60_000, 1).alias(
                "minutes_to_first_observed"
            ),
            F.round((F.col("earliest_updated") - F.col("time")) / 60_000, 1).alias(
                "minutes_to_earliest_captured_version"
            ),
            F.round((F.col("latest_updated") - F.col("time")) / 3_600_000, 2).alias(
                "hours_to_latest_captured_version"
            ),
            "earliest_mag",
            "latest_mag",
            F.round(F.col("latest_mag") - F.col("earliest_mag"), 2).alias(
                "net_mag_change"
            ),
            "earliest_type",
            "latest_type",
            (F.col("earliest_type") != F.col("latest_type")).alias("type_differs"),
        )
    )
