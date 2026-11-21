"""Write a synthetic quake-like dataset (with revisions to dedup) for the benchmark. Usage: gen.py <remote> <rows>"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

remote, rows = sys.argv[1], int(sys.argv[2])
spark = SparkSession.builder.remote(remote).getOrCreate()
regions = F.array(*[F.lit(f"region_{i:02d}") for i in range(50)])
day_ms = 86_400_000
df = spark.range(rows).select(
    # ~20% of rows are revisions of an earlier event id
    F.concat(F.lit("ev"), (F.col("id") % int(rows * 0.8)).cast("string")).alias("id"),
    (F.lit(1_790_000_000_000) + (F.rand(1) * 30 * day_ms)).cast("long").alias("time"),
    (F.lit(1_790_000_000_000) + (F.rand(2) * 31 * day_ms)).cast("long").alias("updated"),
    F.round(F.rand(3) * 7, 1).alias("mag"),
    F.element_at(regions, (F.floor(F.rand(4) * 50) + 1).cast("int")).alias("region"),
    F.round(F.rand(5) * 300, 1).alias("depth"),
    F.round(F.rand(6) * 180 - 90, 3).alias("lat"),
    F.round(F.rand(7) * 360 - 180, 3).alias("lon"),
)
df.repartition(16).write.mode("overwrite").parquet("s3://lake/bench/quakes_synth/")
print("rows:", spark.read.parquet("s3://lake/bench/quakes_synth/").count())
spark.stop()
