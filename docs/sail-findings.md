# Sail in this project

## Spark vs Sail

Measured at commit [`19f9354`](https://github.com/jordandelbar/sail-lake/tree/19f935493a8c9e64250a23efd6e209d0098b39b2); the code has changed a lot since.

Both engines ran in single-node local mode, one after the other, in identical pods (2 CPU and 4Gi of memory, Guaranteed QoS) on the large node. They used the same `pyspark-client==4.0.1` script, the same 20 million row Parquet dataset on versitygw, and default settings. Each query ran once as a warm-up and then three times; the table shows the median. Both engines returned the same answers for all four queries.

|                                         | Sail 0.7.3 | Spark 4.0.1               |
| --------------------------------------- | ---------- | ------------------------- |
| Memory when idle                        | 71 MiB     | 754 MiB                   |
| Peak memory                             | 2,455 MiB  | 4,096 MiB (the pod limit) |
| Pod start to first answered query       | 4.1 s      | 5.4 s                     |
| First session connect                   | 0.5 s      | 3.3 s                     |
| Daily aggregation per region            | 3.65 s     | 2.60 s                    |
| Window dedup, latest revision per event | 6.80 s     | 11.11 s                   |
| Join with a 50 row dimension table      | 0.18 s     | 1.28 s                    |
| Filtered scan and count                 | 0.03 s     | 0.60 s                    |
| Image size                              | 1.7 GB     | 3.1 GB                    |

Memory is the biggest difference. Sail idles at 71 MiB, while Spark's JVM holds about 750 MiB doing nothing and used its whole 4Gi limit under load. For a lake meant to run on small nodes, that matters more than the query times.

The query times are mixed. Sail was 1.6 times faster on the window dedup, which is the heavy step in the real `quakes_events` job, and 7 times faster on the join. Spark was 1.4 times faster on the plain aggregation. Spark ran with its default of 200 shuffle partitions, which is not tuned for 2 cores, so these numbers say little about tuned clusters.

The 0.03 s filtered scan on Sail is too fast for a real read of 20 million rows from S3. It probably hit a cache after the warm-up run. I did not confirm this, so that row is not a fair comparison.

Spark needs `hadoop-aws` and the 558 MB AWS SDK v2 bundle to read S3, which accounts for much of its image size. Most of Sail's 1.7 GB comes from the Python client libraries in the image, not from Sail itself.

This was one run on one machine. Read the numbers as orders of magnitude. To run it again: `just bench`, with `ROWS=...` to change the dataset size. Since the nodes were shrunk, `just bench` runs smaller pods (1.5 CPU and 1.5Gi), so its numbers will not match the table above. Results go to `bench/results.json`.

## Bug: Sail picks Iceberg table locations itself

Reproduced with Sail 0.7.3, standalone and in this project.

Tracked: [#2315](https://github.com/lakehq/sail/issues/2315) (open issue), with a fix in progress in [#2803](https://github.com/lakehq/sail/pull/2803) (open pull request), which gives generated table locations a unique suffix. Neither mentions the ignored `tableProperty("location", ...)` described below.

When a table is created through an Iceberg REST catalog without a location, the REST spec lets the client leave `location` out so the catalog chooses one. Sail fills it in instead, with the namespace's `location` property followed by the table name. Two things go wrong:

- With Lakekeeper's default flat layout, every namespace's location is the warehouse root (`s3://lake/warehouse`). Sail sends `s3://lake/warehouse/same_name` for `lake.scratch_a.same_name`, and creating `lake.scratch_b.same_name` then fails with `Iceberg table already exists at path: s3://lake/warehouse/same_name/`. A table created through Lakekeeper's own API without a location gets `s3://lake/warehouse/<table-uuid>`, which cannot collide.
- `tableProperty("location", ...)` on `writeTo().create()` is ignored, and Sail sends its default path instead. SQL `CREATE TABLE ... USING iceberg LOCATION '...'` is honored.

To reproduce, run `docs/repro-iceberg-location/repro.sh` (needs docker and uv). It starts Apache's reference REST catalog (`apache/iceberg-rest-fixture:1.10.0`) with a local folder as the warehouse, Sail 0.7.3 from the `pysail` wheel, and a small proxy that prints the `location` field of every `CreateTableRequest`:

```text
CreateTableRequest 't': location='file://<tmp>/warehouse/ns_a/t'
CreateTableRequest 't2': location='file://<tmp>/warehouse/ns_a/t2'
CreateTableRequest 't3': location='file://<tmp>/warehouse/custom/t3'
namespace ns_a location:    file://<tmp>/warehouse/ns_a
rest.ns_a.t location:       file://<tmp>/warehouse/ns_a/t
rest.ns_a.t2 location:      file://<tmp>/warehouse/ns_a/t2  (asked for file://<tmp>/warehouse/custom/t2)
rest.ns_a.t3 location:      file://<tmp>/warehouse/custom/t3  (asked for file://<tmp>/warehouse/custom/t3)
```

The reference catalog nests namespace locations (`<warehouse>/ns_a`), so the collision does not show there. The first line still shows Sail sending a location it was not given.

In this project, `Lakehouse.ensure_table()` in `lake/lake/lakehouse.py` works around it by creating every table with SQL and an explicit `s3://lake/warehouse/<namespace>/<table>` location.

## Iceberg MERGE fails in cluster mode once the target has too many files

Seen in this project with Sail 0.7.3; I did not build a standalone reproduction.

The `quakes_events` asset merges new events into `lake.quakes.events` every 15 minutes. After about a day of runs it started failing with `task region requires 9 worker task slots, but the configured maximum is 8 given a maximum of 2 workers with 4 task slots per worker`, although `execution.default_parallelism` is 8.

`EXPLAIN MERGE ...` showed why: the scan of the target table has one file group per data file (`file_groups={9 groups: ...}`), and that count ignores `default_parallelism`. Each MERGE leaves more data files behind, and in cluster mode all tasks of a task region must run at the same time, so the job fails as soon as the table has more files than the cluster has slots, instead of running the tasks in waves. Local mode does not have this limit.

The workaround (`Lakehouse.compact()` in `lake/lake/lakehouse.py`, called after every MERGE) compacts the table with `CALL lake.system.rewrite_data_files(table => 'quakes.events', options => map('rewrite-all', 'true'))`, which Sail supports. After compaction the table has one data file and the runs pass again.

### Partitioned targets

With partitioning, the file count follows the number of partitions. A table partitioned by `days(event_time)` has at least one data file per day, and compaction works within partitions, so a 30-day table keeps 30 files. A MERGE matching on `id` alone then needs 30 slots and fails with `task region requires 30 worker task slots` on this 8-slot cluster.

Adding a partition predicate to the `ON` clause (`AND t.event_time >= <first day of the batch>`) prunes the target scan to one file, and the MERGE runs. Tracked: this is the pruning fixed in [#2476](https://github.com/lakehq/sail/issues/2476) (closed). It works well for live batches that touch a day or two. For a pipeline, two cases need care: a year-long backfill still spans about 365 partitions, and an event whose time USGS revises across midnight would no longer find its old row.

Dynamic partition overwrite worked without any of this. `writeTo().overwritePartitions()`, and `INSERT OVERWRITE` with `spark.sql.sources.partitionOverwriteMode=dynamic`, both replaced the 2 days present in the input on a 30-day table, left the other 28 untouched, and stayed within the slots.

Tracked, related to partitioned writes: [#2563](https://github.com/lakehq/sail/issues/2563) (open) reports that every writer task writes to every partition it receives, so a partitioned write produces about tasks times partitions data files. I did not measure this here.

### Storage shuffle and task regions

Sail's default `flight` shuffle streams data between running tasks instead of writing it out, which avoids an intermediate write and is a large part of why queries are fast. The trade-off is that tasks connected this way run together, so the cluster needs enough slots for all of them at once.

The `storage` backend looked like a way to relax that: its configuration reference describes it as materializing "merged shuffle partitions in object storage before running dependent stages". With it enabled on a separate driver, the same 30-file MERGE still asked for 30 slots on 8.

In the 0.7.3 source, `shuffle_output_mode` in `crates/sail-execution/src/job_graph/planner.rs` returns `Pipelined` for both `flight` and `storage`, and `Blocking` for `celeborn`, while `scalar_subquery_output_mode` already returns `Blocking` for `storage` ([#2490](https://github.com/lakehq/sail/pull/2490) fixed blocking inputs under storage shuffle). Task regions are the stages joined by pipelined outputs, and `enqueue_tasks` in `crates/sail-execution/src/driver/task_assigner/core.rs` checks that a region's worker tasks fit in workers times slots. So in this version the storage backend makes some outputs blocking, while regular shuffles stay pipelined. The main branch had the same `shuffle_output_mode` on November 22, 2026.

I did not try `celeborn`. A region always contains every task of its stages, so my reading is that a single stage wider than the cluster, such as this 30-task scan, would still need all its slots with blocking shuffle. That would be worth confirming with the Sail team.

### What the tracker says

The roadmap discussion ([#1001](https://github.com/lakehq/sail/issues/1001)) points to the distributed processing epic ([#989](https://github.com/lakehq/sail/issues/989)), which covers this area: task scheduling and retry with a bounded number of workers, blocking and hybrid shuffle on local disks or remote storage, and task slots for CPU and memory. The epic was closed on November 5, 2026, and the blocking shuffle issue ([#1021](https://github.com/lakehq/sail/issues/1021), "With local disks and remote storage") on September 4, 2026. I could not tell from the tracker whether blocking scheduling for storage-backed shuffles is planned as a follow-up or handled another way.

I found no open issue about a stage wider than the cluster's task slots, or about the MERGE target scan using one task per data file regardless of `default_parallelism`. Both seem worth raising, with the 30-partition MERGE as a reproduction.

## Smaller rough edges

- Writing timestamps to an Iceberg table fails when the session time zone is `Etc/UTC`, with `Unsupported timezone for Iceberg Timestamptz conversion: Etc/UTC. Timezone must be UTC or +00:00`, although both name the same zone. The session picks the zone up from the container. Setting `spark.sql.session.timeZone` to `UTC` avoids it. Reproduced.
- A Sail server installed without `pyspark-client` in the same environment fails on `createDataFrame` with `configuration not found: spark.sql.execution.pandas.inferPandasDictAsMap`. Sail's Dockerfile installs both packages, but the error does not point there. Reproduced.
- `writeTo()` defaults to Parquet even when the default catalog is an Iceberg REST catalog, so the first attempt fails with `Iceberg REST catalog cannot create 'parquet' tables` until you add `.using("iceberg")`. Reproduced.
- A failed `createOrReplace()` (replacement is not supported through a REST catalog in 0.7.3, as documented) left files in the table location, and Lakekeeper then refused to create the table there. Separately, `DROP TABLE ... PURGE` removed a table from Lakekeeper but left its metadata files. Each happened once and I did not try to reproduce them; the second may be Lakekeeper deleting files later in a background task. Tracked, for the replacement part: [#2981](https://github.com/lakehq/sail/pull/2981) adds atomic table replacement in Iceberg REST catalogs; it was merged on November 20, 2026, after the 0.7.3 release, so this project does not have it yet. With it, `Lakehouse.overwrite()` could replace a table on a schema change in one commit instead of building `<name>__next` and swapping.
- In Kubernetes cluster mode, partitions default to the driver's CPU count, which here was the host's 16 cores, and the first job failed for lack of worker slots. The configuration reference covers this (`execution.default_parallelism`, recommended for cluster mode), but the Kubernetes deployment guide does not mention it. `k8s/sail.yaml` sets it to 8.

These worked without trouble: S3-compatible storage with a custom endpoint, Delta commits through conditional PUT (versitygw rejects a second `If-None-Match: *` write, as it should), Iceberg `MERGE` and `INSERT OVERWRITE`, and Sail starting and cleaning up its own worker pods on Kubernetes.
