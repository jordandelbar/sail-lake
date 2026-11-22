import dagster as dg
from pydantic import Field
from pyspark.sql import functions as F

from lake.lakehouse import Lakehouse
from lake.quakes.events import events, merge_events
from lake.usgs import RAW, read_raw

EVENTS = "quakes.events"

raw_quakes = dg.AssetSpec(
    "raw_quakes",
    group_name="quakes",
    description=f"USGS events as NDJSON at {RAW}, landed continuously by the ingest pipeline",
)


class EventsConfig(dg.Config):
    raw_lookback_days: int = Field(
        default=2,
        ge=1,
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
        batch = events(raw)
        lakehouse.ensure_table(spark, EVENTS, batch)
        merge_events(spark, table, batch)
        compacted = lakehouse.compact(spark, EVENTS)
        batch_rows = batch.count()
        rows = spark.table(table).count()
    return dg.MaterializeResult(
        metadata={
            "table": table,
            "rows": rows,
            "batch_rows": batch_rows,
            "files_compacted": compacted,
        }
    )
