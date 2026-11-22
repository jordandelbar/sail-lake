import math

import dagster as dg
from pydantic import Field
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lake.assets.events import EVENTS, quakes_events
from lake.lakehouse import Lakehouse

MAG_FREQ = "quakes.magnitude_frequency"
SEISMICITY = "quakes.region_seismicity"
# shared by both assets: Mc is a bin of MAG_FREQ, and the b-value formula corrects for this width
MAG_BIN = 0.1


def earthquakes(spark: SparkSession, lakehouse: Lakehouse) -> DataFrame:
    """Earthquakes with a magnitude, plus a copy of all of them under region "ALL"."""
    eq = spark.table(lakehouse.table(EVENTS)).where(
        "event_type = 'earthquake' AND mag IS NOT NULL"
    )
    return eq.unionByName(eq.withColumn("region", F.lit("ALL")))


@dg.asset(
    deps=[quakes_events],
    group_name="quakes",
    description=f"Iceberg {MAG_FREQ}: events per {MAG_BIN} magnitude bin and region, "
    "with the cumulative count at or above each bin (Gutenberg-Richter curve).",
)
def quakes_magnitude_frequency(lakehouse: Lakehouse) -> dg.MaterializeResult:
    with lakehouse.session() as spark:
        at_or_above = Window.partitionBy("region").orderBy(F.desc("mag_bin"))
        mf = (
            earthquakes(spark, lakehouse)
            # round before floor: 2.3 / 0.1 is 22.999... in floating point
            .withColumn(
                "mag_bin", F.floor(F.round(F.col("mag") / MAG_BIN, 6)) * MAG_BIN
            )
            .withColumn("mag_bin", F.round("mag_bin", 1))
            .groupBy("region", "mag_bin")
            .agg(F.count("*").alias("n"))
            .withColumn(
                "n_cum",
                F.sum("n").over(at_or_above.rowsBetween(Window.unboundedPreceding, 0)),
            )
        )
        rows = lakehouse.overwrite(spark, MAG_FREQ, mf)
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
        busiest_bin = Window.partitionBy("region").orderBy(
            F.desc("n"), F.asc("mag_bin")
        )
        mc = (
            spark.table(lakehouse.table(MAG_FREQ))
            .withColumn("rk", F.row_number().over(busiest_bin))
            .where("rk = 1")
            .select("region", F.round(F.col("mag_bin") + 0.2, 1).alias("mc"))
        )
        eq = earthquakes(spark, lakehouse)
        totals = eq.groupBy("region").agg(
            F.count("*").alias("events"), F.max("mag").alias("max_mag")
        )
        above = (
            eq.join(mc, "region")
            .where(F.col("mag") >= F.col("mc") - 1e-9)
            .groupBy("region")
            .agg(
                F.count("*").alias("events_above_mc"),
                F.avg("mag").alias("mean_mag_above_mc"),
            )
        )
        # Aki-Utsu: b = log10(e) / (mean(M >= Mc) - (Mc - bin/2))
        b = F.lit(math.log10(math.e)) / (
            F.col("mean_mag_above_mc") - (F.col("mc") - MAG_BIN / 2)
        )
        seismicity = (
            totals.join(mc, "region", "left")
            .join(above, "region", "left")
            .select(
                "region",
                "events",
                "max_mag",
                "mc",
                "events_above_mc",
                F.when(
                    F.col("events_above_mc") >= config.min_events_for_b, F.round(b, 2)
                )
                .otherwise(F.lit(None))
                .alias("b_value"),
            )
            # no "ALL" row: pooling networks with different completeness breaks the method
            .where("region <> 'ALL'")
        )
        rows = lakehouse.overwrite(spark, SEISMICITY, seismicity)
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
