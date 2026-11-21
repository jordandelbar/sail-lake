set shell := ["bash", "-euo", "pipefail", "-c"]

sail_image := "sail:0.7.3"
dagster_image := "dagster:1.13.31"
code_image := "lake-code:dev"
fetcher_image := "fetcher:dev"
viz_image := "viz:dev"
cluster := "sail-lake"
cpk_image := "registry.k8s.io/cloud-provider-kind/cloud-controller-manager:v0.12.0"

default:
    @just --list

# Cluster, load balancer, images, manifests: everything from scratch
up: cluster lb image deploy

cluster:
    mkdir -p data/s3/lake
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

configmaps:
    kubectl create configmap s3-sink --from-file=ingest/s3-sink/ --dry-run=client -o yaml | kubectl apply -f -
    kubectl create configmap dagster-instance --from-file=dagster/config/ --dry-run=client -o yaml | kubectl apply -f -

deploy: configmaps
    kubectl apply -f k8s/infra.yaml -f k8s/sail.yaml -f k8s/ingest.yaml -f k8s/dagster.yaml -f k8s/lake-code.yaml -f k8s/viz.yaml

# Redeploy user code only, Dagster system untouched
code: (ship code_image "lake/" "lake-code")

# Redeploy the fetcher only
fetcher: (ship fetcher_image "ingest/fetcher/" "fetcher")

# Redeploy the Rill dashboards only
viz: (ship viz_image "viz/" "viz")

# Build one image, load it into kind, restart its deployment
[private]
ship image dir deployment:
    docker build -t {{ image }} {{ dir }}
    kind load docker-image {{ image }} --name {{ cluster }}
    kubectl rollout restart deploy/{{ deployment }}
    kubectl rollout status deploy/{{ deployment }}

# Local venvs for the Python projects (IDE, local runs)
venvs:
    for d in lake dagster sail; do (cd $d && uv sync); done

# Print the UI URLs (LoadBalancer IPs)
ui:
    @for s in dagster-webserver viz; do echo "$s: http://$(kubectl get svc $s -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"; done

# Delete the cluster and the load balancer container (./data is kept)
down:
    kind delete cluster --name {{ cluster }}
    -docker rm -f cloud-provider-kind
