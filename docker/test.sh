#!/usr/bin/env bash
# Smoke test for the Helix 3-node Docker cluster with Kafka protocol.
#
# Starts the cluster, waits for readiness by polling produce, then
# verifies end-to-end produce/consume via Kafka CLI tools.
#
# Usage:
#   cd docker
#   ./test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
NETWORK="docker_helix-net"
KAFKA_IMAGE="confluentinc/cp-kafka:7.7.1"
TOPIC="smoke-test"
TEST_MESSAGE="hello-helix-$(date +%s)"
HEALTH_TIMEOUT=60
READY_TIMEOUT=60

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[FAIL]${NC}  $*"; }

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

# Wait for cluster to be fully ready by retrying a produce until it
# succeeds. This exercises metadata discovery, controller election,
# topic auto-creation, and partition leader election.
wait_for_produce_ready() {
    local topic="$1"
    local message="$2"
    info "Waiting for cluster to accept produce to '$topic'..."
    local deadline=$((SECONDS + READY_TIMEOUT))
    while true; do
        if echo "$message" | docker run --rm -i --network "$NETWORK" \
            "$KAFKA_IMAGE" kafka-console-producer \
            --bootstrap-server helix-node1:9092 \
            --topic "$topic" \
            --request-timeout-ms 2000 \
            --max-block-ms 2000 2>/dev/null; then
            info "Produce succeeded."
            return 0
        fi

        if [ "$SECONDS" -ge "$deadline" ]; then
            error "Timed out waiting for produce readiness."
            exit 1
        fi

        sleep 1
    done
}

# Step 1: Start the cluster.
info "Building and starting 3-node Helix cluster..."
docker compose -f "$COMPOSE_FILE" up --build -d

# Step 2: Wait for healthy, then wait for produce to work.
wait_for_healthy
wait_for_produce_ready "$TOPIC" "$TEST_MESSAGE"
info "Produced: $TEST_MESSAGE"

# Step 3: Consume the message back using direct partition assignment
# (--partition 0 --offset 0 skips consumer group APIs).
info "Consuming message from topic '$TOPIC'..."
CONSUMED=$(docker run --rm --network "$NETWORK" \
    "$KAFKA_IMAGE" \
    kafka-console-consumer --bootstrap-server helix-node1:9092 \
    --topic "$TOPIC" --partition 0 --offset 0 \
    --max-messages 1 --timeout-ms 10000 2>/dev/null) || {
    error "Failed to consume message."
    exit 1
}
info "Consumed: $CONSUMED"

# Step 4: Verify round-trip.
if [ "$CONSUMED" = "$TEST_MESSAGE" ]; then
    info "Round-trip verified: message matches."
    echo ""
    echo -e "${GREEN}=== SMOKE TEST PASSED ===${NC}"
    exit 0
else
    error "Message mismatch!"
    error "  Expected: $TEST_MESSAGE"
    error "  Got:      $CONSUMED"
    echo ""
    echo -e "${RED}=== SMOKE TEST FAILED ===${NC}"
    exit 1
fi
