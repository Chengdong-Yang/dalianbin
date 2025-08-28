#!/usr/bin/env bash
set -euo pipefail

# ========= 基础路径 =========
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="/tmp/8424227"
LOG_FILE="$LOG_DIR/8424227.log"
STAT_DIR="$LOG_DIR"
APP_JAR="$ROOT_DIR/equity-loader/target/equity-loader-1.0.0.jar"               # 你的装载 jar 名称

mkdir -p "$LOG_DIR"

# ========= 比赛平台会注入的环境变量（本地可手动 export）=========
: "${FILE_PATH:?请先 export FILE_PATH，例如：/data/in}"
: "${FILE_NAME_EQUITY:?请先 export FILE_NAME_EQUITY，例如：equity.txt}"
: "${FILE_NAME_RELATION:?请先 export FILE_NAME_RELATION，例如：relation.txt}"

# ========= 回调1参数（平台会验证；本地可覆盖）=========
CB1_URL=${CB1_URL:-"http://82.202.169.44:6000/callback1"}
OMR_ACC=${OMR_ACC:-"8424227"}
OMR_PWD=${OMR_PWD:-"762687ok@"}

echo "[`date +%F' '%T`] === start_product.sh begin ===" | tee -a "$LOG_FILE"

# ========= 运行装载（使用你现有的两个 COPY Loader Bean）=========
JAVA_OPTS="-Xms1g -Xmx2g -Dlogging.file.name=$LOG_FILE"
SPRING_ARGS=(
  "--loader.shards=16"
  "--loader.tablePrefix=tb_customer_equity_"
  "--loader.badFileSuffix=.bad"
  "--loader.statDir=$STAT_DIR"
  "--loader.relation.enabled=true"
  "--loader.relation.tableName=tb_customer_relation"
)

echo "[`date +%F' '%T`] run loader ..." | tee -a "$LOG_FILE"
set +e
java $JAVA_OPTS -jar "$APP_JAR" "${SPRING_ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
RET=$?
set -e
echo "[`date +%F' '%T`] loader exit code = $RET" | tee -a "$LOG_FILE"

# ========= 读取统计数 =========
read_if() { [[ -f "$1" ]] && tr -d '[:space:]' < "$1" || echo "0"; }
EQUITY_OK="$(read_if "$STAT_DIR/equity.ok")"
EQUITY_FAIL="$(read_if "$STAT_DIR/equity.fail")"
RELATION_OK="$(read_if "$STAT_DIR/relation.ok")"
RELATION_FAIL="$(read_if "$STAT_DIR/relation.fail")"

echo "[`date +%F' '%T`] equity_ok=$EQUITY_OK equity_fail=$EQUITY_FAIL relation_ok=$RELATION_OK relation_fail=$RELATION_FAIL" | tee -a "$LOG_FILE"

# ========= 回调1 =========
PAYLOAD=$(cat <<JSON
{
  "omracc":"$OMR_ACC",
  "omrpwd":"$OMR_PWD",
  "filelist":[
    {"filename":"equity","success":"$EQUITY_OK","fail":"$EQUITY_FAIL"},
    {"filename":"relation","success":"$RELATION_OK","fail":"$RELATION_FAIL"}
  ]
}
JSON
)
echo "[`date +%F' '%T`] callback1 → $CB1_URL" | tee -a "$LOG_FILE"
curl -sS -H "Content-Type: application/json" -d "$PAYLOAD" "$CB1_URL" | tee -a "$LOG_FILE"
echo | tee -a "$LOG_FILE"

echo "[`date +%F' '%T`] === start_product.sh done ===" | tee -a "$LOG_FILE"