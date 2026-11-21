"""Time the same queries on a Spark Connect endpoint. Usage: bench.py <remote> -> JSON on stdout."""
import json
import statistics
import sys
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

remote = sys.argv[1]
RUNS = 3

t0 = time.perf_counter()
spark = SparkSession.builder.remote(remote).getOrCreate()
spark.range(1).count()
connect_s = time.perf_counter() - t0

df = spark.read.parquet("s3://lake/bench/quakes_synth/")
dim = spark.createDataFrame([(f"region_{i:02d}", f"zone_{i % 5}") for i in range(50)], "region STRING, zone STRING")


def q_scan():
    return [(df.filter(F.col("mag") >= 4).count(),)]


def q_agg():
    out = (df.groupBy(F.to_date(F.from_unixtime(F.col("time") / 1000)).alias("day"), "region")
           .agg(F.count("*").alias("n"), F.avg("mag").alias("avg_mag"), F.max("mag").alias("max_mag")))
    return sorted((str(r.day), r.region, r.n, round(r.avg_mag, 6), r.max_mag) for r in out.collect())


def q_dedup():
    w = Window.partitionBy("id").orderBy(F.desc("updated"), F.desc("time"))
    latest = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    r = latest.agg(F.count("*").alias("n"), F.sum("mag").alias("s")).collect()[0]
    return [(r.n, round(r.s, 3))]


def q_join():
    out = df.join(dim, "region").groupBy("zone").agg(F.count("*").alias("n"), F.avg("depth").alias("d"))
    return sorted((r.zone, r.n, round(r.d, 6)) for r in out.collect())


results = {"connect_s": round(connect_s, 2), "queries": {}}
for name, q in [("scan_filter", q_scan), ("daily_region_agg", q_agg), ("dedup_window", q_dedup), ("join_dim", q_join)]:
    answer = q()  # warm-up, also the answer compared across engines
    times = []
    for _ in range(RUNS):
        t = time.perf_counter()
        q()
        times.append(time.perf_counter() - t)
    results["queries"][name] = {"median_s": round(statistics.median(times), 2), "answer": answer}
spark.stop()
print(json.dumps(results))
