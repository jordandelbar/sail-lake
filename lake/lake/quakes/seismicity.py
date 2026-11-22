import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# shared: Mc is a bin of the magnitude-frequency table, and the b-value formula corrects for it
MAG_BIN = 0.1


def earthquakes(events: DataFrame) -> DataFrame:
    """Earthquakes with a magnitude, plus a copy of all of them under region "ALL"."""
    eq = events.where("event_type = 'earthquake' AND mag IS NOT NULL")
    return eq.unionByName(eq.withColumn("region", F.lit("ALL")))


def magnitude_frequency(events: DataFrame) -> DataFrame:
    """Per region and MAG_BIN-wide bin: events in the bin (n) and at or above it (n_cum)."""
    at_or_above = Window.partitionBy("region").orderBy(F.desc("mag_bin"))
    return (
        earthquakes(events)
        # round before floor: 2.3 / 0.1 is 22.999... in floating point
        .withColumn("mag_bin", F.floor(F.round(F.col("mag") / MAG_BIN, 6)) * MAG_BIN)
        .withColumn("mag_bin", F.round("mag_bin", 1))
        .groupBy("region", "mag_bin")
        .agg(F.count("*").alias("n"))
        .withColumn(
            "n_cum",
            F.sum("n").over(at_or_above.rowsBetween(Window.unboundedPreceding, 0)),
        )
    )


def seismicity(
    magnitude_frequency: DataFrame, events: DataFrame, min_events_for_b: int
) -> DataFrame:
    """Per region: completeness magnitude Mc (maximum curvature + 0.2) and the Gutenberg-Richter
    b-value (Aki-Utsu maximum likelihood above Mc), reported only with at least
    `min_events_for_b` events above Mc. No "ALL" row: pooling networks with different
    completeness breaks the method."""
    busiest_bin = Window.partitionBy("region").orderBy(F.desc("n"), F.asc("mag_bin"))
    mc = (
        magnitude_frequency.withColumn("rk", F.row_number().over(busiest_bin))
        .where("rk = 1")
        .select("region", F.round(F.col("mag_bin") + 0.2, 1).alias("mc"))
    )
    eq = earthquakes(events)
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
    return (
        totals.join(mc, "region", "left")
        .join(above, "region", "left")
        .select(
            "region",
            "events",
            "max_mag",
            "mc",
            "events_above_mc",
            F.when(F.col("events_above_mc") >= min_events_for_b, F.round(b, 2))
            .otherwise(F.lit(None))
            .alias("b_value"),
        )
        .where("region <> 'ALL'")
    )
