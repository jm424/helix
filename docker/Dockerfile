# Helix distributed log server Docker image.
#
# Build:
#   docker build -t helix-server -f docker/Dockerfile .
#
# Run single node:
#   docker run -p 9092:9092 helix-server
#
# For multi-node setup, see docker-compose.yml.

# Build stage
FROM rust:1.92-bookworm AS builder

# Install protobuf compiler for gRPC code generation.
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only the manifests first to cache dependencies.
COPY Cargo.toml Cargo.lock ./
COPY helix-core/Cargo.toml ./helix-core/
COPY helix-flow/Cargo.toml ./helix-flow/
COPY helix-progress/Cargo.toml ./helix-progress/
COPY helix-wal/Cargo.toml ./helix-wal/
COPY helix-raft/Cargo.toml ./helix-raft/
COPY helix-routing/Cargo.toml ./helix-routing/
COPY helix-runtime/Cargo.toml ./helix-runtime/
COPY helix-server/Cargo.toml ./helix-server/
COPY helix-tier/Cargo.toml ./helix-tier/
COPY helix-tests/Cargo.toml ./helix-tests/
COPY helix-workload/Cargo.toml ./helix-workload/

# Create stub source files to build dependencies.
RUN mkdir -p helix-core/src helix-flow/src helix-progress/src \
    helix-wal/src helix-raft/src helix-routing/src \
    helix-runtime/src helix-server/src helix-tier/src \
    helix-tests/src helix-workload/src && \
    echo "fn main() {}" > helix-core/src/lib.rs && \
    echo "fn main() {}" > helix-flow/src/lib.rs && \
    echo "fn main() {}" > helix-progress/src/lib.rs && \
    echo "fn main() {}" > helix-wal/src/lib.rs && \
    echo "fn main() {}" > helix-raft/src/lib.rs && \
    echo "fn main() {}" > helix-routing/src/lib.rs && \
    echo "fn main() {}" > helix-runtime/src/lib.rs && \
    echo "fn main() {}" > helix-server/src/lib.rs && \
    echo "fn main() {}" > helix-server/src/main.rs && \
    echo "fn main() {}" > helix-tier/src/lib.rs && \
    echo "fn main() {}" > helix-tests/src/lib.rs && \
    echo "fn main() {}" > helix-workload/src/lib.rs

# Build dependencies only (this layer will be cached).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --package helix-server 2>/dev/null || true

# Copy actual source code.
COPY . .

# Touch files to ensure rebuild.
RUN touch helix-core/src/lib.rs helix-flow/src/lib.rs helix-progress/src/lib.rs \
    helix-wal/src/lib.rs helix-raft/src/lib.rs helix-routing/src/lib.rs \
    helix-runtime/src/lib.rs helix-server/src/lib.rs helix-server/src/main.rs \
    helix-tier/src/lib.rs helix-tests/src/lib.rs helix-workload/src/lib.rs

# Build the release binary with BuildKit cache mounts.
# Cargo registry and build artifacts persist between builds, so incremental
# rebuilds only recompile changed crates (~30s instead of 5+ minutes).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --package helix-server && \
    cp /app/target/release/helix-server /tmp/helix-server

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user.
RUN useradd -r -u 1000 -m helix

# Copy the binary from builder.
COPY --from=builder /tmp/helix-server /usr/local/bin/helix-server

# Create data directory.
RUN mkdir -p /var/lib/helix && chown helix:helix /var/lib/helix

USER helix

# Expose ports:
# 9092 - Kafka client API
# 9001 - Raft peer communication
EXPOSE 9092 9001

# Default data directory.
ENV HELIX_DATA_DIR=/var/lib/helix

ENTRYPOINT ["helix-server"]
CMD ["--data-dir", "/var/lib/helix"]
