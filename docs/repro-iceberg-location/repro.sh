#!/usr/bin/env bash
# Standalone: Apache's Iceberg REST fixture (Docker) + Sail 0.7.3 (pysail wheel) + pyspark-client 4.0.1.
# Needs docker and uv. Ports 18181, 18182 and 50099 must be free.
# proxy.py sits between Sail and the catalog and prints the location Sail puts in each CreateTableRequest.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d)
WH="file://$WORK/warehouse"
mkdir -m 777 "$WORK/warehouse"

# same absolute path inside the container, so catalog and Sail agree on file paths
docker run -d --rm --name iceberg-rest-repro -p 18181:8181 -v "$WORK/warehouse:$WORK/warehouse" \
  -e CATALOG_WAREHOUSE="$WH" -e CATALOG_IO__IMPL=org.apache.iceberg.hadoop.HadoopFileIO \
  apache/iceberg-rest-fixture:1.10.0 >/dev/null
until curl -sf http://localhost:18181/v1/config >/dev/null; do sleep 1; done
python3 -I "$HERE/proxy.py" 18182 http://localhost:18181 &
PROXY=$!

# pyspark-client must sit next to pysail, or createDataFrame fails with "configuration not found"
SAIL_CATALOG__LIST="[{type=\"iceberg-rest\", name=\"rest\", uri=\"http://localhost:18182\", warehouse=\"$WH\"}]" \
SAIL_CATALOG__DEFAULT_CATALOG=rest \
  uvx -q --python 3.12 --from pysail==0.7.3 --with pyspark-client==4.0.1 \
  sail spark server --port 50099 >"$WORK/sail.log" 2>&1 &
SAIL=$!
trap 'kill $SAIL $PROXY 2>/dev/null; docker rm -f iceberg-rest-repro >/dev/null 2>&1; rm -rf "$WORK" 2>/dev/null || true' EXIT
until (exec 3<>/dev/tcp/127.0.0.1/50099) 2>/dev/null; do sleep 1; done

uv run -q --no-project --python 3.12 --with pyspark-client==4.0.1 \
  python -I "$HERE/repro.py" sc://localhost:50099 http://localhost:18181 "$WH"
