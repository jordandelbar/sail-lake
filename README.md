# sail-lake

A small lakehouse testbed on kind. A Go fetcher polls the USGS earthquake feed into NATS, Redpanda Connect lands it in S3 (versitygw, backed by `./data`), and Dagster schedules Sail jobs that build Iceberg tables in a Lakekeeper catalog. Rill shows the dashboards.

`just up` builds and deploys everything, `just ui` prints the UI addresses, and `just` lists the other recipes. Requires docker, kind, kubectl, just and uv.

The credentials in `k8s/` are local placeholders. Notes on Sail and a Spark comparison are in [docs/sail-findings.md](docs/sail-findings.md).
