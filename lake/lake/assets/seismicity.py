import dagster as dg
from pydantic import Field

from lake.assets.events import EVENTS, quakes_events
from lake.lakehouse import Lakehouse
from lake.quakes.seismicity import MAG_BIN, magnitude_frequency, seismicity

MAG_FREQ = "quakes.magnitude_frequency"
SEISMICITY = "quakes.region_seismicity"


@dg.asset(
    deps=[quakes_events],
    group_name="quakes",
    description=f"Iceberg {MAG_FREQ}: events per {MAG_BIN} magnitude bin and region, "
    "with the cumulative count at or above each bin (Gutenberg-Richter curve).",
)
def quakes_magnitude_frequency(lakehouse: Lakehouse) -> dg.MaterializeResult:
    with lakehouse.session() as spark:
        rows = lakehouse.overwrite(
            spark, MAG_FREQ, magnitude_frequency(spark.table(lakehouse.table(EVENTS)))
        )
    return dg.MaterializeResult(
        metadata={"table": lakehouse.table(MAG_FREQ), "rows": rows}
    )


class SeismicityConfig(dg.Config):
    min_events_for_b: int = Field(
        default=200,
        ge=1,
        description="Minimum earthquakes at or above Mc to report a b-value; "
        "below that the estimate is mostly noise.",
    )


@dg.asset(
    deps=[quakes_events, quakes_magnitude_frequency],
    group_name="quakes",
    description=f"Iceberg {SEISMICITY}: per region, completeness magnitude Mc "
    "(maximum curvature + 0.2) and Gutenberg-Richter b-value (Aki-Utsu MLE above Mc).",
)
def quakes_region_seismicity(
    config: SeismicityConfig, lakehouse: Lakehouse
) -> dg.MaterializeResult:
    with lakehouse.session() as spark:
        result = seismicity(
            spark.table(lakehouse.table(MAG_FREQ)),
            spark.table(lakehouse.table(EVENTS)),
            config.min_events_for_b,
        )
        rows = lakehouse.overwrite(spark, SEISMICITY, result)
        with_b = (
            spark.table(lakehouse.table(SEISMICITY))
            .where("b_value IS NOT NULL")
            .count()
        )
    return dg.MaterializeResult(
        metadata={
            "table": lakehouse.table(SEISMICITY),
            "rows": rows,
            "regions_with_b_value": with_b,
        }
    )
