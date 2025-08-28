#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/tmp/8424227/8424227.log"
APP_JAR="$(dirname "$0")/equity-query-api/target/equity-query-api-1.1.0.jar"

mkdir -p "$(dirname "$LOG_FILE")"
echo "[`date +%F' '%T`] start_query.sh begin" | tee -a "$LOG_FILE"

java -Xms512m -Xmx1024m \
  -Dlogging.file.name="$LOG_FILE" \
  -jar "$APP_JAR" \
  2>&1 | tee -a "$LOG_FILE" &

echo "[`date +%F' '%T`] query service started" | tee -a "$LOG_FILE"