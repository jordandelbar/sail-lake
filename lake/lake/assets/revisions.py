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
    description=f"Iceberg {REVISIONS}: how USGS revised each live event (every version is in "
    "raw/): versions, time to first report and to final, magnitude and type changes.",
)
def quakes_revisions(lakehouse: Lakehouse) -> dg.MaterializeResult:
    with lakehouse.session() as spark:
        # the backfill only has final versions; rows from before `source` existed are all live
        live = read_raw(spark).where(F.coalesce("source", F.lit("feed")) == "feed")
        oldest = Window.partitionBy("id").orderBy("updated")
        newest = Window.partitionBy("id").orderBy(F.desc("updated"))
        first = (
            live.withColumn("rn", F.row_number().over(oldest))
            .where("rn = 1")
            .select(
                "id",
                "time",
                F.col("updated").alias("first_updated"),
                F.col("mag").alias("first_mag"),
                F.col("type").alias("first_type"),
            )
        )
        last = (
            live.withColumn("rn", F.row_number().over(newest))
            .where("rn = 1")
            .select(
                "id",
                F.col("updated").alias("final_updated"),
                F.col("mag").alias("final_mag"),
                F.col("type").alias("final_type"),
                region_of("place").alias("region"),
            )
        )
        # the same version can sit in raw/ more than once (re-published after a NATS reset)
        versions = live.groupBy("id").agg(F.count_distinct("updated").alias("versions"))
        revisions = (
            first.join(last, "id")
            .join(versions, "id")
            .select(
                "id",
                F.timestamp_millis("time").alias("event_time"),
                "region",
                "versions",
                F.round((F.col("first_updated") - F.col("time")) / 60_000, 1).alias(
                    "minutes_to_first_seen"
                ),
                F.round((F.col("final_updated") - F.col("time")) / 3_600_000, 2).alias(
                    "hours_to_final"
                ),
                "first_mag",
                "final_mag",
                F.round(F.col("final_mag") - F.col("first_mag"), 2).alias("mag_change"),
                "first_type",
                "final_type",
                (F.col("first_type") != F.col("final_type")).alias("type_changed"),
            )
        )
        rows = lakehouse.overwrite(spark, REVISIONS, revisions)
        summary = (
            spark.table(lakehouse.table(REVISIONS))
            .agg(
                F.round(F.avg((F.col("versions") > 1).cast("int")), 3).alias(
                    "share_revised"
                ),
                F.round(F.avg("versions"), 2).alias("avg_versions"),
                F.round(F.avg(F.abs("mag_change")), 3).alias("avg_abs_mag_change"),
            )
            .collect()[0]
        )
    return dg.MaterializeResult(
        metadata={"table": lakehouse.table(REVISIONS), "rows": rows, **summary.asDict()}
    )
