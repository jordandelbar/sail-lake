import math
import os
from collections.abc import Generator
from contextlib import contextmanager

import dagster as dg
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

RAW = "s3://lake/raw/quakes/"
RAW_SCHEMA = (
    "id STRING, time BIGINT, updated BIGINT, mag DOUBLE, magType STRING, place STRING, "
    "type STRING, tsunami INT, lon DOUBLE, lat DOUBLE, depth DOUBLE, source STRING, dt DATE"
)
EVENTS = "lake.quakes.events"
DAILY_REGION = "lake.quakes.daily_region"
MAG_FREQ = "lake.quakes.magnitude_frequency"
SEISMICITY = "lake.quakes.region_seismicity"
REVISIONS = "lake.quakes.revisions"
MAG_BIN = 0.1
# fewer events above Mc than this and the b-value estimate is mostly noise
MIN_EVENTS_FOR_B = 200
# all_day feed: an event stops being revised once it leaves the feed (24h), 2 days covers it
RAW_LOOKBACK_DAYS = 2


@contextmanager
def sail() -> Generator[SparkSession, None, None]:
    """Spark Connect session on Sail, released after the asset body."""
    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def ensure_table(spark: SparkSession, table: str, like: DataFrame) -> None:
    """Create an empty Iceberg table with `like`'s schema, once."""
    _, ns, name = table.split(".")
    like.createOrReplaceTempView("_schema_src")
    # explicit LOCATION: by default Sail uses <namespace location>/<table>, and Lakekeeper's flat
    # layout makes every namespace location the warehouse root, so ns1.t and ns2.t would collide
    # (docs/sail-findings.md, problem 3)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING iceberg "
        f"LOCATION 's3://lake/warehouse/{ns}/{name}' "
        "AS SELECT * FROM _schema_src WHERE false"
    )


# Produced continuously by fetcher.
raw_quakes = dg.AssetSpec(
    "raw_quakes", group_name="quakes", description=f"USGS events as NDJSON at {RAW}"
)


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description=f"Iceberg {EVENTS}: one row per event, latest revision. Merged incrementally.",
)
def quakes_events(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with sail() as spark:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.quakes")
        raw = (
            spark.read.schema(RAW_SCHEMA)
            .json(RAW)
            .where(F.col("dt") >= F.date_sub(F.current_date(), RAW_LOOKBACK_DAYS))
        )
        # USGS revises events and the feed re-emits them with a newer `updated`; keep the last one
        latest = (
            raw.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("id").orderBy(F.desc("updated"))
                ),
            )
            .filter("rn = 1")
            .drop("rn")
        )
        events = latest.select(
            "id",
            F.timestamp_millis("time").alias("event_time"),
            F.timestamp_millis("updated").alias("updated_time"),
            "mag",
            F.col("magType").alias("mag_type"),
            "place",
            # "10 km NW of X, Alaska" -> "Alaska"
            F.trim(
                F.element_at(F.split(F.coalesce("place", F.lit("unknown")), ","), -1)
            ).alias("region"),
            F.col("type").alias("event_type"),
            (F.col("tsunami") == 1).alias("tsunami"),
            "lon",
            "lat",
            F.col("depth").alias("depth_km"),
        )
        ensure_table(spark, EVENTS, events)
        events.createOrReplaceTempView("batch")
        # idempotent: re-running with the same raw data changes nothing
        spark.sql(f"""
            MERGE INTO {EVENTS} AS t USING batch AS b ON t.id = b.id
            WHEN MATCHED AND b.updated_time > t.updated_time THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        # Every MERGE adds data files, and in cluster mode Sail scans the MERGE target with one task
        # per file, all at once: past (max workers x task slots) files the run fails. Compact back
        # to a single file after each merge. ponytail: rewrites the whole table every run, fine at
        # this size; use binpack thresholds or a separate maintenance schedule when it grows.
        compacted = spark.sql(f"""
            CALL lake.system.rewrite_data_files(
              table => '{EVENTS.removeprefix("lake.")}', options => map('rewrite-all', 'true'))
        """).collect()[0]
        batch_rows = events.count()
        rows = spark.table(EVENTS).count()
    return dg.MaterializeResult(
        metadata={
            "table": EVENTS,
            "rows": rows,
            "batch_rows": batch_rows,
            "files_compacted": compacted["rewritten_data_files_count"],
        }
    )


@dg.asset(
    deps=[quakes_events],
    group_name="quakes",
    description=f"Iceberg {DAILY_REGION}: daily event stats per region. Full recompute.",
)
def quakes_daily_region(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with sail() as spark:
        agg = (
            spark.table(EVENTS)
            .groupBy(F.to_date("event_time").alias("day"), "region")
            .agg(
                F.count("*").alias("events"),
                F.round(F.avg("mag"), 2).alias("avg_mag"),
                F.max("mag").alias("max_mag"),
                F.round(F.avg("depth_km"), 1).alias("avg_depth_km"),
            )
        )
        overwrite(spark, DAILY_REGION, agg)
        out = spark.table(DAILY_REGION)
        top = out.orderBy(F.desc("events")).limit(10).toPandas()
        # global agg always yields exactly one row; coalesce covers an empty table
        stats = out.agg(
            F.count("*").alias("rows"),
            F.coalesce(F.sum("events"), F.lit(0)).alias("events"),
        ).collect()[0]
    return dg.MaterializeResult(
        metadata={
            "table": DAILY_REGION,
            "rows": stats["rows"],
            "events": stats["events"],
            "top_regions": dg.MetadataValue.md(
                f"```\n{top.to_string(index=False)}\n```"
            ),
        }
    )


def overwrite(spark: SparkSession, table: str, df: DataFrame) -> int:
    """Replace the table's content with df in one snapshot; returns the new row count."""
    ensure_table(spark, table, df)
    df.createOrReplaceTempView("_overwrite_src")
    # one atomic snapshot: readers see the old or the new table, never half of it
    spark.sql(f"INSERT OVERWRITE {table} SELECT * FROM _overwrite_src")
    return spark.table(table).count()


def earthquakes(spark: SparkSession) -> DataFrame:
    """Earthquakes with a magnitude, plus a copy of all of them under region "ALL"."""
    eq = spark.table(EVENTS).where("event_type = 'earthquake' AND mag IS NOT NULL")
    return eq.unionByName(eq.withColumn("region", F.lit("ALL")))


@dg.asset(
    deps=[quakes_events],
    group_name="quakes",
    description=f"Iceberg {MAG_FREQ}: events per {MAG_BIN} magnitude bin and region, "
    "with the cumulative count at or above each bin (Gutenberg-Richter curve).",
)
def quakes_magnitude_frequency(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    with sail() as spark:
        at_or_above = Window.partitionBy("region").orderBy(F.desc("mag_bin"))
        mf = (
            earthquakes(spark)
            # round before floor: 2.3 / 0.1 is 22.999... in floating point
            .withColumn(
                "mag_bin",
                F.floor(F.round(F.col("mag") / MAG_BIN, 6)) * MAG_BIN,
            )
            .withColumn("mag_bin", F.round("mag_bin", 1))
            .groupBy("region", "mag_bin")
            .agg(F.count("*").alias("n"))
            .withColumn(
                "n_cum",
                F.sum("n").over(at_or_above.rowsBetween(Window.unboundedPreceding, 0)),
            )
        )
        rows = overwrite(spark, MAG_FREQ, mf)
    return dg.MaterializeResult(metadata={"table": MAG_FREQ, "rows": rows})


@dg.asset(
    deps=[quakes_events, quakes_magnitude_frequency],
    group_name="quakes",
    description=f"Iceberg {SEISMICITY}: per region, completeness magnitude Mc "
    "(maximum curvature + 0.2) and Gutenberg-Richter b-value (Aki-Utsu MLE above Mc).",
)
def quakes_region_seismicity(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    with sail() as spark:
        busiest_bin = Window.partitionBy("region").orderBy(
            F.desc("n"), F.asc("mag_bin")
        )
        mc = (
            spark.table(MAG_FREQ)
            .withColumn("rk", F.row_number().over(busiest_bin))
            .where("rk = 1")
            .select("region", F.round(F.col("mag_bin") + 0.2, 1).alias("mc"))
        )
        eq = earthquakes(spark)
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
                F.when(F.col("events_above_mc") >= MIN_EVENTS_FOR_B, F.round(b, 2))
                .otherwise(F.lit(None))
                .alias("b_value"),
            )
        )
        rows = overwrite(spark, SEISMICITY, seismicity.where("region <> 'ALL'"))
        with_b = spark.table(SEISMICITY).where("b_value IS NOT NULL").count()
    return dg.MaterializeResult(
        metadata={"table": SEISMICITY, "rows": rows, "regions_with_b_value": with_b}
    )


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description=f"Iceberg {REVISIONS}: how USGS revised each live event (every version "
    "is in raw/): versions, time to first report and to final, magnitude and type changes.",
)
def quakes_revisions(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with sail() as spark:
        raw = spark.read.schema(RAW_SCHEMA).json(RAW)
        # rows from before the source field existed all came from the live feed
        live = raw.where(F.coalesce("source", F.lit("feed")) == "feed")
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
                F.trim(
                    F.element_at(
                        F.split(F.coalesce("place", F.lit("unknown")), ","), -1
                    )
                ).alias("region"),
            )
        )
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
        rows = overwrite(spark, REVISIONS, revisions)
        summary = (
            spark.table(REVISIONS)
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
        metadata={"table": REVISIONS, "rows": rows, **summary.asDict()}
    )


quakes_job = dg.define_asset_job(
    "quakes",
    selection=[
        quakes_events,
        quakes_daily_region,
        quakes_magnitude_frequency,
        quakes_region_seismicity,
        quakes_revisions,
    ],
)

defs = dg.Definitions(
    assets=[
        raw_quakes,
        quakes_events,
        quakes_daily_region,
        quakes_magnitude_frequency,
        quakes_region_seismicity,
        quakes_revisions,
    ],
    jobs=[quakes_job],
    schedules=[
        dg.ScheduleDefinition(
            name="quakes_15min",
            target=quakes_job,
            cron_schedule="5-59/15 * * * *",
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )
    ],
)
