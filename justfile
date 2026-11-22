set shell := ["bash", "-euo", "pipefail", "-c"]

sail_image := "sail:0.7.3"
dagster_image := "dagster:1.13.31"
code_image := "lake-code:dev"
fetcher_image := "fetcher:dev"
viz_image := "viz:dev"
cluster := "minilake"
cpk_image := "registry.k8s.io/cloud-provider-kind/cloud-controller-manager:v0.12.0"
# namespaces whose pods talk to S3 and need the s3-creds Secret
s3_namespaces := "storage ingest catalog compute viz bench"

default:
    @just --list

# Cluster, load balancer, images, manifests: everything from scratch
up: cluster lb image deploy

cluster:
    mkdir -p data/s3/lake data/catalog/postgres data/dagster/postgres
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
        kubectl -n $ns create secret generic s3-creds --from-env-file=k8s/s3-creds.env --dry-run=client -o yaml | kubectl apply -f -; \
    done

configmaps:
    kubectl -n ingest create configmap s3-sink --from-file=ingest/s3-sink/ --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n orchestration create configmap dagster-instance --from-file=dagster/config/ --dry-run=client -o yaml | kubectl apply -f -

deploy:
    kubectl apply -f k8s/namespaces.yaml
    just secrets configmaps
    kubectl apply -f k8s/storage.yaml -f k8s/ingest.yaml -f k8s/catalog.yaml -f k8s/sail.yaml -f k8s/dagster.yaml -f k8s/lake-code.yaml -f k8s/viz.yaml

# Redeploy user code only, Dagster system untouched
code: (ship code_image "lake/" "orchestration" "lake-code")

# Redeploy the fetcher only
fetcher: (ship fetcher_image "ingest/fetcher/" "ingest" "fetcher")

# Redeploy the Rill dashboards only
viz: (ship viz_image "viz/" "viz" "viz")

# Build one image, load it into kind, restart its deployment
[private]
ship image dir namespace deployment:
    docker build -t {{ image }} {{ dir }}
    kind load docker-image {{ image }} --name {{ cluster }}
    kubectl -n {{ namespace }} rollout restart deploy/{{ deployment }}
    kubectl -n {{ namespace }} rollout status deploy/{{ deployment }}

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
        ip=$(kubectl -n ${s%/*} get svc ${s#*/} -o jsonpath='{.status.loadBalancer.ingress[0].ip}'); \
        echo "${s#*/}: http://$ip$([ ${s#*/} = lakekeeper ] && echo /ui)"; \
    done

# Delete the cluster and the load balancer container (./data is kept)
down:
    kind delete cluster --name {{ cluster }}
    -docker rm -f cloud-provider-kind
