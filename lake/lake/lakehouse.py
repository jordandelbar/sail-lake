import uuid

import dagster as dg
from pydantic import PrivateAttr
from pyspark.sql import DataFrame, SparkSession

from lake.quakes.raw import RAW_SCHEMA


class Lakehouse(dg.ConfigurableResource):
    """Sail (Spark Connect), the Iceberg catalog it writes to, and the raw landing area.

    An asset's key is its table: key ["quakes", "events"] is table lake.quakes.events. Assets
    write to their own key and read upstream tables through the upstream asset's key.

    Dagster opens one Spark Connect session per step (setup_for_execution) and always stops it
    afterwards, even on failure: stopping is what releases Sail's worker pods. DataFrames are
    only usable inside the step that read them.
    """

    spark_remote: str
    catalog: str = "lake"
    warehouse: str = "s3://lake/warehouse"
    raw_path: str = "s3://lake/raw/quakes/"

    _spark: SparkSession | None = PrivateAttr(default=None)

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._spark = SparkSession.builder.remote(self.spark_remote).getOrCreate()

    def teardown_after_execution(self, context: dg.InitResourceContext) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            raise RuntimeError(
                "no Spark session: Lakehouse is only usable during a run step"
            )
        return self._spark

    def table(self, key: dg.AssetKey) -> str:
        """Asset key ["quakes", "events"] -> "lake.quakes.events"."""
        return self._fq(self._name(key))

    def read(self, key: dg.AssetKey) -> DataFrame:
        """The current content of an asset's table."""
        return self.spark.table(self.table(key))

    def read_raw(self) -> DataFrame:
        """Every version of every event, as landed by fetcher -> NATS -> s3-sink."""
        return self.spark.read.schema(RAW_SCHEMA).json(self.raw_path)

    def ensure_table(self, key: dg.AssetKey, like: DataFrame) -> None:
        self._ensure(self._name(key), like)

    def overwrite(self, key: dg.AssetKey, df: DataFrame) -> int:
        return self._overwrite(self._name(key), df)

    def compact(self, key: dg.AssetKey) -> int:
        return self._compact(self._name(key))

    @staticmethod
    def _name(key: dg.AssetKey) -> str:
        if len(key.path) != 2:
            raise ValueError(
                f"asset key {key.to_user_string()} is not [namespace, table]"
            )
        return ".".join(key.path)

    def _fq(self, name: str) -> str:
        return f"{self.catalog}.{name}"

    def _ensure(self, name: str, like: DataFrame) -> None:
        """Create an empty Iceberg table with `like`'s schema, once.

        Each creation gets its own directory: Lakekeeper refuses non-empty locations, and a
        dropped table's files are not always removed, so a recreated table (schema change,
        rollback) must never reuse an earlier path.
        """
        ns, table = name.split(".")
        table_dir = table.removesuffix("__next")
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{ns}")
        like.createOrReplaceTempView("_schema_src")
        # explicit LOCATION: by default Sail uses <namespace location>/<table>, and Lakekeeper's flat
        # layout makes every namespace location the warehouse root, so ns1.t and ns2.t would collide
        # (docs/sail-findings.md)
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self._fq(name)} USING iceberg "
            f"LOCATION '{self.warehouse}/{ns}/{table_dir}/{uuid.uuid4().hex[:12]}' "
            "AS SELECT * FROM _schema_src WHERE false"
        )

    def _overwrite(self, name: str, df: DataFrame) -> int:
        """Replace the table's content with df in one snapshot; returns the new row count.

        Overwritten tables are fully recomputed. On a schema change the replacement is built
        completely as "<name>__next" first, then swapped in (drop + rename), so readers keep the
        previous result until it exists and a failed build leaves it untouched. Sail does not
        apply overwriteSchema to catalog tables, hence the swap. Merged tables (quakes.events)
        never go through here.
        """
        fq = self._fq(name)
        if (
            self.spark.catalog.tableExists(fq)
            and self.spark.table(fq).schema.simpleString() != df.schema.simpleString()
        ):
            nxt = f"{name}__next"
            self.spark.sql(
                f"DROP TABLE IF EXISTS {self._fq(nxt)} PURGE"
            )  # failed earlier swap
            self._fill(nxt, df)
            self.spark.sql(f"DROP TABLE {fq} PURGE")
            self.spark.sql(f"ALTER TABLE {self._fq(nxt)} RENAME TO {fq}")
        else:
            self._fill(name, df)
        return self.spark.table(fq).count()

    def _fill(self, name: str, df: DataFrame) -> None:
        self._ensure(name, df)
        df.createOrReplaceTempView("_overwrite_src")
        # one atomic snapshot: readers see the old or the new content, never half of it
        self.spark.sql(
            f"INSERT OVERWRITE {self._fq(name)} SELECT * FROM _overwrite_src"
        )

    def _compact(self, name: str) -> int:
        """Rewrite the table into as few data files as possible; returns files rewritten.

        In cluster mode Sail scans a MERGE target with one task per data file, all at once, so a
        table merged into repeatedly must be compacted or the MERGE eventually needs more task
        slots than the cluster has. ponytail: rewrites the whole table every call, fine at this
        size; use binpack thresholds or a separate maintenance schedule when it grows.
        """
        result = self.spark.sql(f"""
            CALL {self.catalog}.system.rewrite_data_files(
              table => '{name}', options => map('rewrite-all', 'true'))
        """).collect()[0]
        return result["rewritten_data_files_count"]
