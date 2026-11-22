import dagster as dg
from pyspark.sql import functions as F

from lake.assets.events import raw_quakes
from lake.lakehouse import Lakehouse
from lake.quakes.revisions import revisions
from lake.usgs import read_raw

REVISIONS = "quakes.revisions"


@dg.asset(
    deps=[raw_quakes],
    group_name="quakes",
    description=f"Iceberg {REVISIONS}: per live event, the versions this pipeline captured "
    "(earliest/latest captured, net changes; see lake.quakes.revisions).",
)
def quakes_revisions(lakehouse: Lakehouse) -> dg.MaterializeResult:
    with lakehouse.session() as spark:
        rows = lakehouse.overwrite(spark, REVISIONS, revisions(read_raw(spark)))
        summary = (
            spark.table(lakehouse.table(REVISIONS))
            .agg(
                F.round(F.avg((F.col("versions_captured") > 1).cast("int")), 3).alias(
                    "share_with_several_captured_versions"
                ),
                F.round(F.avg("versions_captured"), 2).alias("avg_versions_captured"),
                F.round(F.avg(F.abs("net_mag_change")), 3).alias(
                    "avg_abs_net_mag_change"
                ),
            )
            .collect()[0]
        )
    return dg.MaterializeResult(
        metadata={"table": lakehouse.table(REVISIONS), "rows": rows, **summary.asDict()}
    )
