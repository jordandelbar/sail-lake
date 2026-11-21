"""Raw quake NDJSON -> latest version per event -> daily stats per region, as Parquet."""
import os

from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.remote(os.environ.get("SPARK_REMOTE", "sc://sail:50051")).getOrCreate()

raw = spark.read.json("s3://lake/raw/quakes/")
# USGS revises events, the feed re-emits them with a newer `updated`; keep the last one
latest = (raw.withColumn("rn", F.row_number().over(Window.partitionBy("id").orderBy(F.desc("updated"))))
          .filter("rn = 1").drop("rn"))

agg = (latest
       .withColumn("day", F.to_date(F.from_unixtime(F.col("time") / 1000)))
       .withColumn("region", F.trim(F.element_at(F.split(F.coalesce("place", F.lit("unknown")), ","), -1)))
       .groupBy("day", "region")
       .agg(F.count("*").alias("events"), F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.max("mag").alias("max_mag"), F.round(F.avg("depth"), 1).alias("avg_depth_km")))

agg.write.mode("overwrite").parquet("s3://lake/agg/quakes_daily_region/")
spark.read.parquet("s3://lake/agg/quakes_daily_region/").orderBy(F.desc("events")).show(15, truncate=False)
