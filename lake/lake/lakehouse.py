from collections.abc import Generator
from contextlib import contextmanager

import dagster as dg
from pyspark.sql import DataFrame, SparkSession


class Lakehouse(dg.ConfigurableResource):
    """Sail (Spark Connect) and the Iceberg catalog it writes to.

    Tables are named "<namespace>.<table>"; the catalog prefix is added here.
    """

    spark_remote: str
    catalog: str = "lake"
    warehouse: str = "s3://lake/warehouse"

    @contextmanager
    def session(self) -> Generator[SparkSession, None, None]:
        """Spark Connect session on Sail, released after the block."""
        spark = SparkSession.builder.remote(self.spark_remote).getOrCreate()
        try:
            yield spark
        finally:
            spark.stop()

    def table(self, name: str) -> str:
        """ "quakes.events" -> "lake.quakes.events"."""
        return f"{self.catalog}.{name}"

    def ensure_table(self, spark: SparkSession, name: str, like: DataFrame) -> None:
        """Create an empty Iceberg table with `like`'s schema, once."""
        ns, table = name.split(".")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{ns}")
        like.createOrReplaceTempView("_schema_src")
        # explicit LOCATION: by default Sail uses <namespace location>/<table>, and Lakekeeper's flat
        # layout makes every namespace location the warehouse root, so ns1.t and ns2.t would collide
        # (docs/sail-findings.md)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.table(name)} USING iceberg "
            f"LOCATION '{self.warehouse}/{ns}/{table}' "
            "AS SELECT * FROM _schema_src WHERE false"
        )

    def overwrite(self, spark: SparkSession, name: str, df: DataFrame) -> int:
        """Replace the table's content with df in one snapshot; returns the new row count."""
        self.ensure_table(spark, name, df)
        df.createOrReplaceTempView("_overwrite_src")
        # one atomic snapshot: readers see the old or the new table, never half of it
        spark.sql(f"INSERT OVERWRITE {self.table(name)} SELECT * FROM _overwrite_src")
        return spark.table(self.table(name)).count()

    def compact(self, spark: SparkSession, name: str) -> int:
        """Rewrite the table into as few data files as possible; returns files rewritten.

        In cluster mode Sail scans a MERGE target with one task per data file, all at once, so a
        table merged into repeatedly must be compacted or the MERGE eventually needs more task
        slots than the cluster has. ponytail: rewrites the whole table every call, fine at this
        size; use binpack thresholds or a separate maintenance schedule when it grows.
        """
        result = spark.sql(f"""
            CALL {self.catalog}.system.rewrite_data_files(
              table => '{name}', options => map('rewrite-all', 'true'))
        """).collect()[0]
        return result["rewritten_data_files_count"]
