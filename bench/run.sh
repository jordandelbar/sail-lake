#!/usr/bin/env bash
# Spark vs Sail on identical pods, one engine at a time. Pauses the Dagster daemon so scheduled
# runs don't spawn Sail workers mid-benchmark. Writes bench/results.json.
set -euo pipefail
cd "$(dirname "$0")/.."
ROWS=${ROWS:-20000000}
client() { kubectl -n orchestration exec -i deploy/lake-code -- python - "$@"; }
pod() { kubectl -n bench get pod -l app="$1" -o jsonpath='{.items[0].metadata.name}'; }
cg() { kubectl -n bench exec "$(pod "$1")" -- cat "/sys/fs/cgroup/$2"; }

kubectl apply -f bench/engines.yaml >/dev/null
kubectl -n orchestration scale deploy/dagster-daemon --replicas=0 >/dev/null
trap 'kubectl -n orchestration scale deploy/dagster-daemon --replicas=1 >/dev/null; kubectl -n bench scale deploy/sail-bench deploy/spark-bench --replicas=0 >/dev/null' EXIT

echo "generating ${ROWS} rows (once per size) ..." >&2
client sc://sail.compute:50051 "$ROWS" < bench/gen.py >&2

echo "{" > bench/results.json
for engine in sail spark; do
  echo "== $engine" >&2
  start=$(date +%s.%N)
  kubectl -n bench scale "deploy/$engine-bench" --replicas=1 >/dev/null
  kubectl -n bench rollout status "deploy/$engine-bench" --timeout=300s >/dev/null
  # ready = first query answered
  until client "sc://$engine-bench.bench:15002" < <(echo 'import sys; from pyspark.sql import SparkSession as S; S.builder.remote(sys.argv[1]).getOrCreate().range(1).count()') 2>/dev/null; do sleep 1; done
  startup=$(echo "$(date +%s.%N) - $start" | bc)
  idle=$(cg "$engine-bench" memory.current)
  res=$(client "sc://$engine-bench.bench:15002" < bench/bench.py)
  peak=$(cg "$engine-bench" memory.peak)
  printf '"%s": {"startup_s": %.1f, "idle_mem_mib": %d, "peak_mem_mib": %d, "run": %s},\n' \
    "$engine" "$startup" $((idle / 1048576)) $((peak / 1048576)) "$res" >> bench/results.json
  kubectl -n bench scale "deploy/$engine-bench" --replicas=0 >/dev/null
  kubectl -n bench wait --for=delete pod -l app="$engine-bench" --timeout=120s >/dev/null 2>&1 || true
done
sed -i '$ s/,$//' bench/results.json && echo "}" >> bench/results.json
echo "done: bench/results.json" >&2
