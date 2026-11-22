import dagster as dg
from pydantic import Field
from pyspark.sql import functions as F

from lake.lakehouse import Lakehouse
from lake.quakes.events import events, merge_events

raw_quakes = dg.AssetSpec(
    ["quakes", "raw"],
    group_name="quakes",
    description="USGS events as NDJSON in the raw landing area (Lakehouse.raw_path), "
    "written continuously by the ingest pipeline",
)


class EventsConfig(dg.Config):
    raw_lookback_days: int = Field(
        default=2,
        ge=1,
        description="Days of raw data (by landing date) merged per run. The live feed stops "
        "revising an event once it leaves the 24h feed, so 2 days covers every revision.",
    )


@dg.asset(
    key=["quakes", "events"],
    deps=[raw_quakes],
    group_name="quakes",
    description="One row per event, latest revision. Merged incrementally.",
)
def quakes_events(
    context: dg.AssetExecutionContext, config: EventsConfig, lakehouse: Lakehouse
) -> dg.MaterializeResult:
    key = context.asset_key
    raw = lakehouse.read_raw().where(
        F.col("dt") >= F.date_sub(F.current_date(), config.raw_lookback_days)
    )
    batch = events(raw)
    lakehouse.ensure_table(key, batch)
    merge_events(lakehouse.table(key), batch)
    return dg.MaterializeResult(
        metadata={
            "table": lakehouse.table(key),
            "files_compacted": lakehouse.compact(key),
            "batch_rows": batch.count(),
            "rows": lakehouse.read(key).count(),
        }
    )
