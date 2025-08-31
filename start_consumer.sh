#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/logs}"
mkdir -p "$LOG_DIR"

JAR="${CONSUMER_JAR:-$ROOT_DIR/mq-consumer-service/target/mq-consumer-service-1.0.0.jar}"
LOG_FILE="$LOG_DIR/consumer_$(date +%Y%m%d_%H%M%S).log"

echo "[start] java -Xms512m -Xmx1024m -jar $JAR"
nohup java -Xms512m -Xmx1024m -jar "$JAR" >> "$LOG_FILE" 2>&1 &
echo "logs: $LOG_FILE"
