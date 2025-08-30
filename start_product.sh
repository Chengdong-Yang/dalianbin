#!/usr/bin/env bash
set -euo pipefail

# ========= 基础路径 =========
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="/tmp/8424227"
LOG_FILE="$LOG_DIR/8424227.log"
APP_JAR="$ROOT_DIR/equity-loader/target/equity-loader-1.0.0.jar"

mkdir -p "$LOG_DIR"

# ========= 赛方会注入的变量（本地可手动 export）=========
: "${FILE_PATH:?请先 export FILE_PATH，例如：/data/in}"
: "${FILE_NAME_EQUITY:?请先 export FILE_NAME_EQUITY，例如：equity.txt}"
: "${FILE_NAME_RELATION:?请先 export FILE_NAME_RELATION，例如：relation.txt}"

# ========= Java 侧会负责回调1（在 BootstrapRunner 里），这里只做启动 =========
echo "[`date +%F' '%T`] === start_product.sh begin ===" | tee -a "$LOG_FILE"

JAVA_OPTS="-Xms1g -Xmx2g -Dlogging.file.name=$LOG_FILE"

# 这些开关/参数将传给 Spring Boot（仍然可被 application.yml 覆盖）
SPRING_ARGS=(
  "--loader.enabled=true"                 # 跑资产文件
  "--loader.relation.enabled=true"        # 跑关系文件
  "--loader.shards=16"
  "--loader.tablePrefix=tb_customer_equity_"
  "--loader.badFileSuffix=.bad"
  "--loader.statDir=${STAT_DIR:-/tmp/8424227}"
  "--loader.relation.tableName=tb_customer_relation"
  # 回调参数也交给 Java（BootstrapRunner 里 postWithRetry）：
  "--callback.cb1-url=${CB1_URL:-http://82.202.169.44:6000/callback1}"
  "--callback.omr-acc=${OMR_ACC:-8424227}"
  "--callback.omr-pwd=${OMR_PWD:-762687ok@}"
)

echo "[`date +%F' '%T`] run loader ..." | tee -a "$LOG_FILE"
java $JAVA_OPTS -jar "$APP_JAR" "${SPRING_ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
RET=${PIPESTATUS[0]}   # 取到 java 的退出码

echo "[`date +%F' '%T`] loader exit code = $RET" | tee -a "$LOG_FILE"
echo "[`date +%F' '%T`] === start_product.sh done ===" | tee -a "$LOG_FILE"

exit $RET
