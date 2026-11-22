set shell := ["bash", "-euo", "pipefail", "-c"]

sail_image := "sail:0.7.3"
dagster_image := "dagster:1.13.31"
code_image := "lake-code:dev"
fetcher_image := "fetcher:dev"
viz_image := "viz:dev"
cluster := "minilake"
# every kubectl call targets this cluster, whatever the current context is
kubectl := "kubectl --context kind-" + cluster
cpk_image := "registry.k8s.io/cloud-provider-kind/cloud-controller-manager:v0.12.0"
# namespaces whose pods talk to S3 and need the s3-creds Secret
s3_namespaces := "storage ingest catalog compute viz bench"

default:
    @just --list

# Cluster, load balancer, images, manifests: everything from scratch
up: cluster lb image deploy

cluster:
    mkdir -p data/s3/lake data/catalog/postgres data/dagster/postgres data/nats
    kind get clusters | grep -qx {{ cluster }} || kind create cluster --config kind.yaml

# LoadBalancer Services for kind: runs on the host (not in the cluster), needs the docker socket
lb:
    docker inspect cloud-provider-kind >/dev/null 2>&1 || docker run -d --name cloud-provider-kind \
        --restart unless-stopped --network kind -v /var/run/docker.sock:/var/run/docker.sock {{ cpk_image }}

# Build all images and load them into kind
image:
    docker build -t {{ sail_image }} sail/
    docker build -t {{ dagster_image }} dagster/
    docker build -t {{ code_image }} lake/
    docker build -t {{ fetcher_image }} ingest/fetcher/
    docker build -t {{ viz_image }} viz/
    kind load docker-image {{ sail_image }} {{ dagster_image }} {{ code_image }} {{ fetcher_image }} {{ viz_image }} --name {{ cluster }}

# s3-creds Secret in every namespace that uses S3, from k8s/s3-creds.env
secrets:
    for ns in {{ s3_namespaces }}; do \
        {{ kubectl }} -n $ns create secret generic s3-creds --from-env-file=k8s/s3-creds.env --dry-run=client -o yaml | {{ kubectl }} apply -f -; \
    done

configmaps:
    {{ kubectl }} -n ingest create configmap s3-sink --from-file=ingest/s3-sink/ --dry-run=client -o yaml | {{ kubectl }} apply -f -
    {{ kubectl }} -n orchestration create configmap dagster-instance --from-file=dagster/config/ --dry-run=client -o yaml | {{ kubectl }} apply -f -

# Apply everything, roll out what changed (images, mounted config), wait until ready.
# The catalog init Job is idempotent and Jobs are immutable, so it is recreated every time.
deploy:
    {{ kubectl }} apply -f k8s/namespaces.yaml
    just secrets configmaps
    {{ kubectl }} -n catalog delete job lakekeeper-init --ignore-not-found
    {{ kubectl }} apply -f k8s/storage.yaml -f k8s/ingest.yaml -f k8s/catalog.yaml -f k8s/sail.yaml -f k8s/dagster.yaml -f k8s/lake-code.yaml -f k8s/viz.yaml
    just stamp orchestration lake-code {{ code_image }} "dagster/config/*"
    just stamp orchestration dagster-webserver {{ dagster_image }} "dagster/config/*"
    just stamp orchestration dagster-daemon {{ dagster_image }} "dagster/config/*"
    just stamp compute sail {{ sail_image }}
    just stamp ingest fetcher {{ fetcher_image }}
    just stamp ingest s3-sink "" "ingest/s3-sink/*"
    just stamp viz viz {{ viz_image }}
    for ns in storage ingest catalog compute orchestration viz; do {{ kubectl }} -n $ns rollout status deploy --timeout=5m; done
    {{ kubectl }} -n catalog wait --for=condition=complete job/lakekeeper-init --timeout=5m

# Record a deployment's image content (filesystem layers + runtime config: entrypoint, cmd, env,
# user, workdir) and mounted-config hash in its pod template: Kubernetes rolls the pods out only
# when one of them changed. Not the image ID: Docker build attestations change it on every build.
[private]
stamp namespace deployment image="" config="":
    id=$([ -n "{{ image }}" ] && docker image inspect -f '{{{{json .RootFS.Layers}}{{{{json .Config}}' {{ image }} | sha256sum | cut -c1-12 || echo none); \
    cfg=$([ -n "{{ config }}" ] && cat {{ config }} | sha256sum | cut -c1-12 || echo none); \
    {{ kubectl }} -n {{ namespace }} patch deploy/{{ deployment }} \
        -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"minilake/image\":\"$id\",\"minilake/config\":\"$cfg\"}}}}}"

# Redeploy user code only, Dagster system untouched
code: (ship code_image "lake/" "orchestration" "lake-code" "dagster/config/*")

# Redeploy the fetcher only
fetcher: (ship fetcher_image "ingest/fetcher/" "ingest" "fetcher")

# Redeploy the Rill dashboards only
viz: (ship viz_image "viz/" "viz" "viz")

# Build one image, load it into kind, roll its deployment out if the image changed
[private]
ship image dir namespace deployment config="":
    docker build -t {{ image }} {{ dir }}
    kind load docker-image {{ image }} --name {{ cluster }}
    just stamp {{ namespace }} {{ deployment }} {{ image }} "{{ config }}"
    {{ kubectl }} -n {{ namespace }} rollout status deploy/{{ deployment }}

# End-to-end check: launch the quakes job and wait for it to succeed
smoke timeout="600":
    #!/usr/bin/env bash
    set -euo pipefail
    k() { {{ kubectl }} "$@"; }
    q() { k -n orchestration exec deploy/dagster-postgres -- psql -U dagster -tAc "$1"; }
    t0=$(q "select now() at time zone 'utc'")
    # the CLI exits non-zero after submitting (known quirk); the run itself is what we check
    k -n orchestration exec deploy/dagster-webserver -- dagster job launch -w /opt/dagster/config/workspace.yaml -l lake -j quakes >/dev/null 2>&1 || true
    deadline=$((SECONDS + {{ timeout }}))
    while :; do
        status=$(q "select status from runs where pipeline_name = 'quakes' and create_timestamp > '$t0' order by create_timestamp desc limit 1")
        case "$status" in
            SUCCESS) echo "smoke: quakes run succeeded"; exit 0 ;;
            FAILURE|CANCELED) echo "smoke: quakes run $status"; exit 1 ;;
        esac
        [ $SECONDS -lt $deadline ] || { echo "smoke: no finished run after {{ timeout }}s (status: ${status:-none})"; exit 1; }
        sleep 5
    done

# Publish a year of USGS history into the ingest path (one-off Job, safe to re-run)
backfill:
    {{ kubectl }} -n ingest delete job quakes-backfill --ignore-not-found
    {{ kubectl }} apply -f k8s/backfill.yaml
    {{ kubectl }} -n ingest wait --for=condition=complete job/quakes-backfill --timeout=30m
    {{ kubectl }} -n ingest logs job/quakes-backfill --tail=3

# Spark vs Sail on identical single-node pods (writes bench/results.json); ROWS=... to resize
bench:
    docker build -t spark-bench:4.0.1 bench/spark/
    kind load docker-image spark-bench:4.0.1 --name {{ cluster }}
    ./bench/run.sh

# Local venvs for the Python projects (IDE, local runs)
venvs:
    for d in lake dagster sail; do (cd $d && uv sync); done

# Print the UI URLs (LoadBalancer IPs)
ui:
    @for s in orchestration/dagster-webserver viz/viz catalog/lakekeeper; do \
        ip=$({{ kubectl }} -n ${s%/*} get svc ${s#*/} -o jsonpath='{.status.loadBalancer.ingress[0].ip}'); \
        echo "${s#*/}: http://$ip$([ ${s#*/} = lakekeeper ] && echo /ui)"; \
    done

# Delete the cluster and the load balancer container (./data is kept)
down:
    kind delete cluster --name {{ cluster }}
    -docker rm -f cloud-provider-kind
