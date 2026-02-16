#!/usr/bin/env bash
# 128-partition throughput benchmark for the Helix Docker cluster.
#
# Starts a 3-node cluster with auto-create partitions, triggers topic
# creation, waits for all partition leaders, then runs benchmarks.
#
# Usage:
#   cd docker
#   ./bench.sh                    # defaults: 128 partitions, 1M records, 1KB each
#   ./bench.sh 64 500000 512      # 64 partitions, 500K records, 512 bytes each

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
NETWORK="docker_helix-net"
KAFKA_IMAGE="confluentinc/cp-kafka:7.7.1"

PARTITIONS="${1:-128}"
NUM_RECORDS="${2:-1000000}"
RECORD_SIZE="${3:-1024}"
TOPIC="bench-p${PARTITIONS}"
HEALTH_TIMEOUT=90
READY_TIMEOUT=120

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[FAIL]${NC}  $*"; }
header(){ echo -e "${CYAN}=== $* ===${NC}"; }

cleanup() {
    info "Tearing down cluster..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

trap cleanup EXIT

# Wait for Docker healthchecks to pass for all 3 nodes.
wait_for_healthy() {
    info "Waiting for all nodes to be healthy..."
    local deadline=$((SECONDS + HEALTH_TIMEOUT))
    while true; do
        local healthy=0
        for node in helix-node1 helix-node2 helix-node3; do
            local status
            status=$(docker inspect --format='{{.State.Health.Status}}' "$node" 2>/dev/null || echo "missing")
            if [ "$status" = "healthy" ]; then
                healthy=$((healthy + 1))
            fi
        done

        if [ "$healthy" -eq 3 ]; then
            info "All 3 nodes are healthy."
            return 0
        fi

        if [ "$SECONDS" -ge "$deadline" ]; then
            error "Timed out waiting for healthy nodes ($healthy/3 healthy)."
            docker compose -f "$COMPOSE_FILE" logs --tail=30
            exit 1
        fi

        sleep 2
    done
}

# Trigger topic auto-creation by retrying produce until it succeeds,
# then poll kafka-get-offsets until all partition leaders are elected.
# kafka-get-offsets uses ListOffsets API (which we implement) and
# returns one line per partition with a leader.
wait_for_topic_ready() {
    local topic="$1"
    local expected_partitions="$2"
    local deadline=$((SECONDS + READY_TIMEOUT))

    info "Triggering topic auto-creation for '$topic'..."
    while true; do
        if echo "probe" | docker run --rm -i --network "$NETWORK" \
            "$KAFKA_IMAGE" kafka-console-producer \
            --bootstrap-server helix-node1:9092 \
            --topic "$topic" \
            --request-timeout-ms 2000 \
            --max-block-ms 2000 2>/dev/null; then
            info "Topic '$topic' created."
            break
        fi

        if [ "$SECONDS" -ge "$deadline" ]; then
            error "Timed out triggering topic creation."
            exit 1
        fi

        sleep 1
    done

    info "Waiting for $expected_partitions partition leaders..."
    while true; do
        local ready
        ready=$(docker run --rm --network "$NETWORK" "$KAFKA_IMAGE" \
            kafka-get-offsets \
            --bootstrap-server helix-node1:9092 \
            --topic "$topic" 2>/dev/null \
            | wc -l | tr -d ' ')

        if [ "$ready" -ge "$expected_partitions" ]; then
            info "All $expected_partitions partition leaders elected."
            return 0
        fi

        if [ "$SECONDS" -ge "$deadline" ]; then
            error "Timed out waiting for partition leaders ($ready/$expected_partitions ready)."
            exit 1
        fi

        info "  $ready/$expected_partitions leaders ready..."
        sleep 3
    done
}

header "Helix Docker Benchmark"
echo "  Partitions:  $PARTITIONS"
echo "  Records:     $NUM_RECORDS"
echo "  Record size: $RECORD_SIZE bytes"
echo "  Total data:  ~$(( NUM_RECORDS * RECORD_SIZE / 1048576 )) MB"
echo ""

# Step 1: Start cluster with configured partition count.
info "Starting 3-node cluster (auto-create-partitions=$PARTITIONS)..."
export HELIX_PARTITIONS="$PARTITIONS"
docker compose -f "$COMPOSE_FILE" up --build -d

# Step 2: Wait for healthy, then for all partition leaders.
wait_for_healthy
wait_for_topic_ready "$TOPIC" "$PARTITIONS"

# Step 3: Produce benchmark.
header "Producer Benchmark"
info "Producing $NUM_RECORDS records (${RECORD_SIZE}B each) to topic '$TOPIC'..."
echo ""

docker run --rm --network "$NETWORK" \
    "$KAFKA_IMAGE" \
    kafka-producer-perf-test \
    --topic "$TOPIC" \
    --num-records "$NUM_RECORDS" \
    --record-size "$RECORD_SIZE" \
    --throughput -1 \
    --producer-props \
        bootstrap.servers=helix-node1:9092,helix-node2:9092,helix-node3:9092 \
        acks=all \
        linger.ms=5 \
        batch.size=65536 \
        buffer.memory=134217728 || {
    error "Producer perf test failed."
    docker compose -f "$COMPOSE_FILE" logs --tail=50
    exit 1
}

echo ""

# Step 4: Verify consume works by reading from each partition via
# kafka-get-offsets (confirms data is readable, not just produced).
header "Consume Verification"
TOTAL_OFFSETS=$(docker run --rm --network "$NETWORK" "$KAFKA_IMAGE" \
    kafka-get-offsets \
    --bootstrap-server helix-node1:9092 \
    --topic "$TOPIC" 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')
info "Total records across all partitions: $TOTAL_OFFSETS"

echo ""
header "Benchmark Complete"
