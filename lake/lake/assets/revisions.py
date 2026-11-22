import dagster as dg
from pyspark.sql import functions as F

from lake.assets.events import raw_quakes
from lake.lakehouse import Lakehouse
from lake.quakes.revisions import revisions


@dg.asset(
    key=["quakes", "revisions"],
    deps=[raw_quakes],
    group_name="quakes",
    description="Per live event, the versions this pipeline captured "
    "(earliest/latest captured, net changes; see lake.quakes.revisions).",
)
def quakes_revisions(
    context: dg.AssetExecutionContext, lakehouse: Lakehouse
) -> dg.MaterializeResult:
    key = context.asset_key
    rows = lakehouse.overwrite(key, revisions(lakehouse.read_raw()))
    summary = (
        lakehouse.read(key)
        .agg(
            F.round(F.avg((F.col("versions_captured") > 1).cast("int")), 3).alias(
                "share_with_several_captured_versions"
            ),
            F.round(F.avg("versions_captured"), 2).alias("avg_versions_captured"),
            F.round(F.avg(F.abs("net_mag_change")), 3).alias("avg_abs_net_mag_change"),
        )
        .collect()[0]
    )
    return dg.MaterializeResult(
        metadata={"table": lakehouse.table(key), "rows": rows, **summary.asDict()}
    )
