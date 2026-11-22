"""Where the raw layer lives and how to read it (the format itself is in lake.quakes.raw)."""

from pyspark.sql import DataFrame, SparkSession

from lake.quakes.raw import RAW_SCHEMA

RAW = "s3://lake/raw/quakes/"


def read_raw(spark: SparkSession) -> DataFrame:
    """Every version of every event, as landed by fetcher -> NATS -> s3-sink."""
    return spark.read.schema(RAW_SCHEMA).json(RAW)
