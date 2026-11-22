import dagster as dg
from pyspark.sql import Window
from pyspark.sql import functions as F

from lake.assets.events import raw_quakes
from lake.lakehouse import Lakehouse
from lake.usgs import read_raw, region_of

REVISIONS = "quakes.revisions"


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description=f"Iceberg {REVISIONS}: per live event, the versions this pipeline captured. "
    "Earliest and latest are the earliest and latest *captured* versions, not USGS's first "
    "publication or final answer; changes are net changes between those two.",
)
def quakes_revisions(lakehouse: Lakehouse) -> dg.MaterializeResult:
    with lakehouse.session() as spark:
        # the backfill only has final versions; rows from before `source` existed are all live
        live = read_raw(spark).where(F.coalesce("source", F.lit("feed")) == "feed")
        oldest = Window.partitionBy("id").orderBy("updated")
        newest = Window.partitionBy("id").orderBy(F.desc("updated"))
        earliest = (
            live.withColumn("rn", F.row_number().over(oldest))
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
            live.withColumn("rn", F.row_number().over(newest))
            .where("rn = 1")
            .select(
                "id",
                F.col("updated").alias("latest_updated"),
                F.col("mag").alias("latest_mag"),
                F.col("type").alias("latest_type"),
                region_of("place").alias("region"),
            )
        )
        # the same version can sit in raw/ more than once (re-published after a NATS reset).
        # observed is null for rows captured before the fetcher recorded it; if any row of an event
        # lacks it, its first observation may predate the recorded ones, so report it as unknown
        per_event = live.groupBy("id").agg(
            F.count_distinct("updated").alias("versions_captured"),
            F.when(F.count("observed") == F.count("*"), F.min("observed")).alias(
                "first_observed"
            ),
        )
        revisions = (
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
        rows = lakehouse.overwrite(spark, REVISIONS, revisions)
        summary = (
            spark.table(lakehouse.table(REVISIONS))
            .agg(
                F.round(F.avg((F.col("versions_captured") > 1).cast("int")), 3).alias(
                    "share_with_several_captured_versions"
                ),
                F.round(F.avg("versions_captured"), 2).alias("avg_versions_captured"),
                F.round(F.avg(F.abs("net_mag_change")), 3).alias(
                    "avg_abs_net_mag_change"
                ),
            )
            .collect()[0]
        )
    return dg.MaterializeResult(
        metadata={"table": lakehouse.table(REVISIONS), "rows": rows, **summary.asDict()}
    )
