#!/bin/bash
# =============================================================================
# Creates Kafka topics for Hermes
# Run AFTER Kafka is healthy: make kafka-topics
# =============================================================================

set -e   # Exit immediately if any command fails

KAFKA_CONTAINER="hermes-kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "→ Creating Kafka topics for Hermes..."

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic incidents.raw \
  --partitions 3 \
  --replication-factor 1

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic incidents.enriched \
  --partitions 3 \
  --replication-factor 1

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic incidents.alerts \
  --partitions 1 \
  --replication-factor 1

echo "✓ Topics created. Current topics:"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list