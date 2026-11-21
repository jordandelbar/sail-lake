# Upstream "quick-start" image (pip wheel). No public image exists.
FROM python:3.12-slim
RUN pip install --no-cache-dir "pysail==0.7.3" "pyspark-client==4.0.1"
ENTRYPOINT ["/usr/local/bin/sail"]
