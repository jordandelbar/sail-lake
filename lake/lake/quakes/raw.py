"""The raw layer's contract: every version of every event, as landed by the ingest pipeline."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Explicit: schema inference samples files, and older files lack newer fields (source, observed).
# dt is the hive partition from the path (landing date).
RAW_SCHEMA = (
    "id STRING, time BIGINT, updated BIGINT, mag DOUBLE, magType STRING, place STRING, "
    "type STRING, tsunami INT, lon DOUBLE, lat DOUBLE, depth DOUBLE, source STRING, "
    "observed BIGINT, dt DATE"
)


def region_of(place: str) -> Column:
    """ "10 km NW of X, Alaska" -> "Alaska"; a missing place gives "unknown"."""
    return F.trim(F.element_at(F.split(F.coalesce(place, F.lit("unknown")), ","), -1))


def live(raw: DataFrame) -> DataFrame:
    """Rows captured from the live feed. Rows from before `source` existed were all live."""
    return raw.where(F.coalesce("source", F.lit("feed")) == "feed")
