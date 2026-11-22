import dagster as dg
from pydantic import Field

from lake.assets.events import quakes_events
from lake.lakehouse import Lakehouse
from lake.quakes.seismicity import magnitude_frequency, seismicity


@dg.asset(
    key=["quakes", "magnitude_frequency"],
    deps=[quakes_events],
    group_name="quakes",
    description="Events per magnitude bin and region, with the cumulative count at or above "
    "each bin (Gutenberg-Richter curve).",
)
def quakes_magnitude_frequency(
    context: dg.AssetExecutionContext, lakehouse: Lakehouse
) -> dg.MaterializeResult:
    key = context.asset_key
    rows = lakehouse.overwrite(
        key, magnitude_frequency(lakehouse.read(quakes_events.key))
    )
    return dg.MaterializeResult(metadata={"table": lakehouse.table(key), "rows": rows})


class SeismicityConfig(dg.Config):
    min_events_for_b: int = Field(
        default=200,
        ge=1,
        description="Minimum earthquakes at or above Mc to report a b-value; "
        "below that the estimate is mostly noise.",
    )


@dg.asset(
    key=["quakes", "region_seismicity"],
    deps=[quakes_events, quakes_magnitude_frequency],
    group_name="quakes",
    description="Per region, completeness magnitude Mc (maximum curvature + 0.2) and "
    "Gutenberg-Richter b-value (Aki-Utsu MLE above Mc).",
)
def quakes_region_seismicity(
    context: dg.AssetExecutionContext, config: SeismicityConfig, lakehouse: Lakehouse
) -> dg.MaterializeResult:
    key = context.asset_key
    result = seismicity(
        lakehouse.read(quakes_magnitude_frequency.key),
        lakehouse.read(quakes_events.key),
        config.min_events_for_b,
    )
    rows = lakehouse.overwrite(key, result)
    with_b = lakehouse.read(key).where("b_value IS NOT NULL").count()
    return dg.MaterializeResult(
        metadata={
            "table": lakehouse.table(key),
            "rows": rows,
            "regions_with_b_value": with_b,
        }
    )
