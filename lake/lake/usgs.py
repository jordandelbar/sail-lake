"""Raw USGS layer: NDJSON landed by the ingest pipeline (fetcher -> NATS -> s3-sink)."""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

RAW = "s3://lake/raw/quakes/"
# Explicit: schema inference samples files, and older files lack newer fields (source).
# dt is the hive partition from the path (landing date).
RAW_SCHEMA = (
    "id STRING, time BIGINT, updated BIGINT, mag DOUBLE, magType STRING, place STRING, "
    "type STRING, tsunami INT, lon DOUBLE, lat DOUBLE, depth DOUBLE, source STRING, dt DATE"
)


def read_raw(spark: SparkSession) -> DataFrame:
    """Every version of every event, as landed."""
    return spark.read.schema(RAW_SCHEMA).json(RAW)


def region_of(place: str) -> Column:
    """ "10 km NW of X, Alaska" -> "Alaska"; a missing place gives "unknown"."""
    return F.trim(F.element_at(F.split(F.coalesce(place, F.lit("unknown")), ","), -1))
