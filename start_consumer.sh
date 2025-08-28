#!/usr/bin/env bash
# start_consumer.sh - 后台启动 MQ 消费服务；当连续一段时间无新消息时回调并停止
# 用法：
#   export CONSUMER_JAR=.../mq-consumer-service-1.0.0.jar
#   ./start_consumer.sh

set -euo pipefail

#####################################
# 1) 可配置参数（可用环境变量覆盖）
#####################################
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 消费者 JAR
CONSUMER_JAR="${CONSUMER_JAR:-}"
if [[ -z "${CONSUMER_JAR}" ]]; then
  CANDIDATE="$(ls -1 ${ROOT_DIR}/mq-consumer-service/target/mq-consumer-service-1.0.0.jar ${ROOT_DIR}/mq-consumer-service/target/*mq-consumer-service*.jar 2>/dev/null | head -n1 || true)"
  if [[ -z "${CANDIDATE}" ]]; then
    echo "ERROR: 未找到 MQ 消费 JAR。请设置 CONSUMER_JAR 或把 jar 放在 ${ROOT_DIR}/ 或 ${ROOT_DIR}/target/ 下（名称包含 mq-consumer）" >&2
    exit 1
  fi
  CONSUMER_JAR="${CANDIDATE}"
fi

JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx1024m}"

# 日志
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/logs}"
mkdir -p "${LOG_DIR}"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/consumer_${TS}.log"

# 统计目录（和 application.yml 对齐）
export STAT_DIR="${STAT_DIR:-/tmp/8424227}"
MQ_OK_FILE="${STAT_DIR}/mq.ok"

# “静默窗口”秒数：这段时间 mq.ok 不增长就认为无新消息
QUIET_SECS="${QUIET_SECS:-60}"
# 轮询间隔
POLL_INTERVAL="${POLL_INTERVAL:-5}"
# 停止消费者的等待时间（秒）
STOP_WAIT_SECS="${STOP_WAIT_SECS:-20}"

# 回调
CB2_URL="${CB2_URL:-http://82.202.169.44:6000/callback2}"
OMR_ACC="${OMR_ACC:-8424227}"
OMR_PWD="${OMR_PWD:-762687ok@}"

# Pulsar 连接相关（供应用读取）
export PULSAR_URL="${PULSAR_URL:-pulsar://127.0.0.1:6650}"
unset PULSAR_TOKEN
export PULSAR_TOPIC="${PULSAR_TOPIC:-skill_competition}"
export SUBSCRIPTION_NAME="${SUBSCRIPTION_NAME:-equity-sub}"

#####################################
# 2) 工具函数
#####################################
read_success() {
  if [[ -f "${MQ_OK_FILE}" ]]; then
    tr -d '[:space:]' < "${MQ_OK_FILE}" 2>/dev/null || echo 0
  else
    echo 0
  fi
}

post_callback() {
  local success="$1"
  local payload
  payload=$(cat <<JSON
{"omracc":"${OMR_ACC}","omrpwd":"${OMR_PWD}","success":"${success}"}
JSON
)
  echo "[callback2] POST ${CB2_URL} payload=${payload}" | tee -a "${LOG_FILE}"
  for i in 1 2 3; do
    if curl -sS -m 10 -H "Content-Type: application/json" -d "${payload}" "${CB2_URL}" >> "${LOG_FILE}" 2>&1 ; then
      echo "[callback2] OK" | tee -a "${LOG_FILE}"
      return 0
    fi
    echo "[callback2] 第 ${i} 次失败，重试中..." | tee -a "${LOG_FILE}"
    sleep 2
  done
  echo "[callback2] 失败（多次重试无果）" | tee -a "${LOG_FILE}"
  return 1
}

graceful_stop() {
  local pid="$1"
  if kill -0 "${pid}" 2>/dev/null; then
    echo "[stop] 优雅停止 pid=${pid}" | tee -a "${LOG_FILE}"
    kill "${pid}" 2>/dev/null || true
    for ((i=0;i<${STOP_WAIT_SECS};i++)); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        echo "[stop] 进程已退出" | tee -a "${LOG_FILE}"
        return 0
      fi
      sleep 1
    done
    echo "[stop] 超时，强制 kill -9 ${pid}" | tee -a "${LOG_FILE}"
    kill -9 "${pid}" 2>/dev/null || true
  fi
}

#####################################
# 3) 启动消费者（后台）
#####################################
echo "[start] java ${JAVA_OPTS} -jar ${CONSUMER_JAR}" | tee -a "${LOG_FILE}"
nohup java ${JAVA_OPTS} -jar "${CONSUMER_JAR}" >> "${LOG_FILE}" 2>&1 &
APP_PID=$!
echo "[start] consumer pid=${APP_PID}" | tee -a "${LOG_FILE}"

#####################################
# 4) 静默窗口探测：mq.ok 在 QUIET_SECS 内不增长 => 回调并停止
#####################################
prev_val="$(read_success)"
prev_ts="$(date +%s)"

while true; do
  # 进程是否还在
  if ! kill -0 "${APP_PID}" 2>/dev/null; then
    echo "[watch] 消费者进程已退出，立即回调。" | tee -a "${LOG_FILE}"
    break
  fi

  sleep "${POLL_INTERVAL}"

  curr_val="$(read_success)"
  now_ts="$(date +%s)"

  if [[ "${curr_val}" != "${prev_val}" ]]; then
    # 有新消息
    echo "[watch] mq.ok: ${prev_val} -> ${curr_val}" | tee -a "${LOG_FILE}"
    prev_val="${curr_val}"
    prev_ts="${now_ts}"
    continue
  fi

  # 没有增长，检查静默时间
  idle=$(( now_ts - prev_ts ))
  if (( idle >= QUIET_SECS )); then
    echo "[watch] 连续 ${idle}s 无新消息，触发回调并停止消费者。" | tee -a "${LOG_FILE}"
    break
  fi
done

SUCCESS="$(read_success)"
echo "[stat] SUCCESS=${SUCCESS}" | tee -a "${LOG_FILE}"

# 回调
post_callback "${SUCCESS}"

# 停消费者（如果还在）
graceful_stop "${APP_PID}"

exit 0