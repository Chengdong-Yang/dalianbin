#!/usr/bin/env bash
# 停止联机/批量/消息三个 Java 服务：app-loader.jar / app-consumer.jar / app-query.jar
# 策略：优雅停止(SIGTERM) -> 超时强杀(SIGKILL)
# 日志：/tmp/8424227/8424227.log

set -euo pipefail

LOG_FILE="/tmp/8424227/8424227.log"
mkdir -p "$(dirname "$LOG_FILE")"

echo "[`date +%F' '%T`] stop.sh begin" | tee -a "$LOG_FILE"

# 你提交的 jar 名称（和 start_* 脚本一致）
DIR="$(cd "$(dirname "$0")" && pwd)"
JARS=(
  "app-loader.jar"
  "app-consumer.jar"
  "app-query.jar"
)

# 优雅停止 + 强杀
stop_one() {
  local jar="$1"
  local pattern="$DIR/$jar"

  # 找进程
  # - 注意使用 [j]ava + jar 路径进行过滤，避免误杀
  local pids
  pids=$(pgrep -f "java .*${pattern}" || true)

  if [[ -z "$pids" ]]; then
    echo "[`date +%F' '%T`] $jar not running" | tee -a "$LOG_FILE"
    return 0
  fi

  echo "[`date +%F' '%T`] Stopping $jar (PIDs: $pids) ..." | tee -a "$LOG_FILE"

  # 发 TERM
  kill $pids 2>/dev/null || true

  # 等待最多 30s
  local waited=0
  while kill -0 $pids 2>/dev/null; do
    sleep 1
    waited=$((waited+1))
    if (( waited >= 30 )); then
      echo "[`date +%F' '%T`] $jar still running after ${waited}s, kill -9 ..." | tee -a "$LOG_FILE"
      kill -9 $pids 2>/dev/null || true
      break
    fi
  done

  if kill -0 $pids 2>/dev/null; then
    echo "[`date +%F' '%T`] $jar force-killed" | tee -a "$LOG_FILE"
  else
    echo "[`date +%F' '%T`] $jar stopped" | tee -a "$LOG_FILE"
  fi
}

for j in "${JARS[@]}"; do
  stop_one "$j"
done

echo "[`date +%F' '%T`] stop.sh done" | tee -a "$LOG_FILE"