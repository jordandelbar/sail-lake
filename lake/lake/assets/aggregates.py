import dagster as dg
from pyspark.sql import functions as F

from lake.assets.events import EVENTS, quakes_events
from lake.lakehouse import Lakehouse

DAILY_REGION = "quakes.daily_region"


@dg.asset(
    deps=[quakes_events],
    group_name="quakes",
    description="Iceberg quakes.daily_region: daily event stats per region. Full recompute.",
)
def quakes_daily_region(lakehouse: Lakehouse) -> dg.MaterializeResult:
    with lakehouse.session() as spark:
        agg = (
            spark.table(lakehouse.table(EVENTS))
            .groupBy(F.to_date("event_time").alias("day"), "region")
            .agg(
                F.count("*").alias("events"),
                F.round(F.avg("mag"), 2).alias("avg_mag"),
                F.max("mag").alias("max_mag"),
                F.round(F.avg("depth_km"), 1).alias("avg_depth_km"),
            )
        )
        rows = lakehouse.overwrite(spark, DAILY_REGION, agg)
        out = spark.table(lakehouse.table(DAILY_REGION))
        top = out.orderBy(F.desc("events")).limit(10).toPandas()
        # coalesce covers an empty table: sum over no rows is null
        events = out.agg(F.coalesce(F.sum("events"), F.lit(0))).collect()[0][0]
    return dg.MaterializeResult(
        metadata={
            "table": lakehouse.table(DAILY_REGION),
            "rows": rows,
            "events": events,
            "top_regions": dg.MetadataValue.md(
                f"```\n{top.to_string(index=False)}\n```"
            ),
        }
    )
