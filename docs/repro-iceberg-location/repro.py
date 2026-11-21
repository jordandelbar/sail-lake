"""Sail + Iceberg REST catalog: where do new tables land?

Usage: repro.py <sail sc:// url> <catalog http url> <warehouse uri>
"""

import json
import sys
import urllib.request

from pyspark.sql import SparkSession

remote, catalog, warehouse = sys.argv[1:4]
spark = SparkSession.builder.remote(remote).getOrCreate()


def location(ns: str, table: str) -> str:
    with urllib.request.urlopen(f"{catalog}/v1/namespaces/{ns}/tables/{table}") as r:
        return json.load(r)["metadata"]["location"]


df = spark.createDataFrame([(1, "a")], "id INT, name STRING")
spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.ns_a")

# 1. No location given. The REST spec lets the client omit it so the catalog picks one, but the
#    proxy shows Sail sending <namespace location>/<table>. With Lakekeeper's default flat layout the
#    namespace location is the warehouse root, so ns_a.t and ns_b.t get the same path.
with urllib.request.urlopen(f"{catalog}/v1/namespaces/ns_a") as r:
    print("namespace ns_a location:   ", json.load(r)["properties"].get("location"))
df.writeTo("rest.ns_a.t").using("iceberg").create()
print("rest.ns_a.t location:      ", location("ns_a", "t"))

# 2. tableProperty("location") is ignored.
asked = f"{warehouse}/custom/t2"
df.writeTo("rest.ns_a.t2").using("iceberg").tableProperty("location", asked).create()
print(f"rest.ns_a.t2 location:      {location('ns_a', 't2')}  (asked for {asked})")

# 3. SQL LOCATION is honored.
df.createOrReplaceTempView("src")
asked = f"{warehouse}/custom/t3"
spark.sql(
    f"CREATE TABLE rest.ns_a.t3 USING iceberg LOCATION '{asked}' AS SELECT * FROM src"
).collect()
print(f"rest.ns_a.t3 location:      {location('ns_a', 't3')}  (asked for {asked})")
spark.stop()
