import dagster as dg
from pyspark.sql import functions as F

from lake.assets.events import quakes_events
from lake.lakehouse import Lakehouse
from lake.quakes.events import daily_region


@dg.asset(
    key=["quakes", "daily_region"],
    deps=[quakes_events],
    group_name="quakes",
    description="Daily event stats per region. Full recompute.",
)
def quakes_daily_region(
    context: dg.AssetExecutionContext, lakehouse: Lakehouse
) -> dg.MaterializeResult:
    key = context.asset_key
    rows = lakehouse.overwrite(key, daily_region(lakehouse.read(quakes_events.key)))
    out = lakehouse.read(key)
    top = out.orderBy(F.desc("events")).limit(10).toPandas()
    # coalesce covers an empty table: sum over no rows is null
    events = out.agg(F.coalesce(F.sum("events"), F.lit(0))).collect()[0][0]
    return dg.MaterializeResult(
        metadata={
            "table": lakehouse.table(key),
            "rows": rows,
            "events": events,
            "top_regions": dg.MetadataValue.md(
                f"```\n{top.to_string(index=False)}\n```"
            ),
        }
    )
