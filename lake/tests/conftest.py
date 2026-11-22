import datetime as dt

import pytest
from pysail.spark import SparkConnectServer
from pyspark.sql import DataFrame, SparkSession

from lake.quakes.raw import RAW_SCHEMA


@pytest.fixture(scope="session")
def spark():
    """Sail embedded in the test process: the production engine, no cluster needed."""
    server = SparkConnectServer()
    server.start(background=True)
    address = server.listening_address  # None until the server has started
    assert address is not None, "embedded Sail server did not start"
    _, port = address
    session = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    yield session
    session.stop()
    server.stop()


def raw_row(
    id: str,
    updated: int,
    *,
    time: int = 1_000_000,
    mag: float | None = 1.0,
    type: str = "earthquake",
    place: str = "10 km N of X, Alaska",
    source: str | None = "feed",
    observed: int | None = None,
) -> tuple:
    """A row in RAW_SCHEMA order."""
    return (
        id,
        time,
        updated,
        mag,
        "ml",
        place,
        type,
        0,
        -150.0,
        61.0,
        10.0,
        source,
        observed,
        dt.date(2026, 11, 22),
    )


def raw_df(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    return spark.createDataFrame(rows, RAW_SCHEMA)
