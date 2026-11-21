SAIL_IMAGE := sail:0.7.3
DAGSTER_IMAGE := dagster:1.13.31
CODE_IMAGE := lake-code:dev
FETCHER_IMAGE := fetcher:dev
CLUSTER := sail-lake
CPK_IMAGE := registry.k8s.io/cloud-provider-kind/cloud-controller-manager:v0.12.0

up: cluster lb image deploy

cluster:
	mkdir -p data/s3/lake
	kind get clusters | grep -qx $(CLUSTER) || kind create cluster --config kind.yaml

# LoadBalancer Services for kind: runs on the host (not in the cluster), needs the docker socket.
lb:
	docker inspect cloud-provider-kind >/dev/null 2>&1 || docker run -d --name cloud-provider-kind \
		--restart unless-stopped --network kind -v /var/run/docker.sock:/var/run/docker.sock $(CPK_IMAGE)

image:
	docker build -t $(SAIL_IMAGE) sail/
	docker build -t $(DAGSTER_IMAGE) dagster/
	docker build -t $(CODE_IMAGE) lake/
	docker build -t $(FETCHER_IMAGE) ingest/fetcher/
	kind load docker-image $(SAIL_IMAGE) $(DAGSTER_IMAGE) $(CODE_IMAGE) $(FETCHER_IMAGE) --name $(CLUSTER)

configmaps:
	kubectl create configmap s3-sink --from-file=ingest/s3-sink/ --dry-run=client -o yaml | kubectl apply -f -
	kubectl create configmap dagster-instance --from-file=dagster/config/ --dry-run=client -o yaml | kubectl apply -f -

deploy: configmaps
	kubectl apply -f k8s/infra.yaml -f k8s/sail.yaml -f k8s/ingest.yaml -f k8s/dagster.yaml -f k8s/lake-code.yaml

# Redeploy user code only, Dagster system untouched.
code:
	docker build -t $(CODE_IMAGE) lake/
	kind load docker-image $(CODE_IMAGE) --name $(CLUSTER)
	kubectl rollout restart deploy/lake-code
	kubectl rollout status deploy/lake-code

fetcher:
	docker build -t $(FETCHER_IMAGE) ingest/fetcher/
	kind load docker-image $(FETCHER_IMAGE) --name $(CLUSTER)
	kubectl rollout restart deploy/fetcher
	kubectl rollout status deploy/fetcher

# Local venvs for the Python projects (IDE, local runs).
venvs:
	for d in lake dagster sail; do (cd $$d && uv sync); done

ui:
	@echo http://$$(kubectl get svc dagster-webserver -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

down:
	kind delete cluster --name $(CLUSTER)
	docker rm -f cloud-provider-kind

.PHONY: up cluster lb image configmaps deploy code fetcher venvs ui down
