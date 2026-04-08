FROM python:3.12-slim

# iproute2 for tc netem latency simulation
RUN apt-get update && \
    apt-get install -y --no-install-recommends iproute2 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY snapspec/ snapspec/
COPY experiments/ experiments/

# Default: run a storage node
ENTRYPOINT ["python", "-m", "snapspec.node"]
