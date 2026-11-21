SAIL_IMAGE := sail:0.7.3
CLUSTER := sail-lake

up: cluster image deploy

cluster:
	mkdir -p data/s3/lake
	kind get clusters | grep -qx $(CLUSTER) || kind create cluster --config kind.yaml

image:
	docker build -t $(SAIL_IMAGE) sail/
	kind load docker-image $(SAIL_IMAGE) --name $(CLUSTER)

configmaps:
	kubectl create configmap ingest --from-file=ingest/ --dry-run=client -o yaml | kubectl apply -f -
	kubectl create configmap jobs --from-file=jobs/ --dry-run=client -o yaml | kubectl apply -f -

deploy: configmaps
	kubectl apply -f k8s/infra.yaml -f k8s/sail.yaml -f k8s/ingest.yaml

aggregate: configmaps
	kubectl delete job aggregate --ignore-not-found
	kubectl apply -f k8s/job-aggregate.yaml
	kubectl wait --for=condition=complete --timeout=300s job/aggregate
	kubectl logs job/aggregate --tail=25

down:
	kind delete cluster --name $(CLUSTER)

.PHONY: up cluster image configmaps deploy aggregate down
