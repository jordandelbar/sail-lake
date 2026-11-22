# Code location "lake": user code only, served over gRPC to the Dagster system.
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never PATH=/app/.venv/bin:$PATH
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
ENV DAGSTER_HOME=/opt/dagster/home
RUN mkdir -p $DAGSTER_HOME
COPY lake ./lake
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "lake.definitions", "-d", "/app"]
