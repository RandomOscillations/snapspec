FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# iproute2 for tc netem latency simulation, plus build deps for the C++ blockstores.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        iproute2 \
        pybind11-dev \
        sqlite3 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY CMakeLists.txt ./
COPY README.md ./
COPY src/ src/
COPY snapspec/ snapspec/
COPY experiments/ experiments/
COPY demo_remote/ demo_remote/
COPY docker/ docker/

RUN pip install --no-cache-dir pyyaml pytest pytest-asyncio aiomysql
RUN cmake -B build -S . -DSNAPSPEC_BUILD_TESTS=OFF && \
    cmake --build build --target _blockstore -j

CMD ["bash"]
