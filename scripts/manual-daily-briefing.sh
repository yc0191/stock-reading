#!/usr/bin/env bash
# 데일리 브리핑(아침 판, 마감 후 판)이 수집 자료 → 시세처리기·뉴스 응답 → 브리핑 → LLM → briefing.daily.generated
# → 발송기 드라이런 로그까지 끝까지 흐르는지 사람이 확인하는 도구다.
#
# 이 실행만의 자원을 만들고 끝나면 그것만 지운다.
# - Kafka: 일회용 컨테이너(127.0.0.1 의 빈 포트, 볼륨 없음). 개발용 Kafka 는 쓰지 않는다.
# - DB: 개발용 Postgres 컨테이너 안의 새 데이터베이스 daily_e2e_<시각>_<pid>. 다른 데이터베이스는 읽지도 쓰지도 않는다.
# - 서비스: market·news·notifier(bootJar)와 briefing 을 임시 폴더에서 띄워 저장소의 .env 를 읽지 않게 한다.
# 실제 시세·뉴스 검색 API 는 부르지 않는다. 수집 자료와 시작 신호는 scripts/daily_e2e.py 가 TEST 접두 가짜 값으로 넣는다.
# 발송기는 드라이런만 한다(NOTIFIER_SEND_ENABLED=false). 수신 주소는 example.com 가짜 주소다.
#
# 메일: DAILY_E2E_SMTP_SINK=1 이면 이 실행의 발송기 한 인스턴스만 발송을 켜고 127.0.0.1 의 가짜 SMTP 수신 서버로 보낸다.
#       받은 메일은 결과 폴더의 mail-<시각>/ 아래 .eml 로 남는다. 외부 SMTP·실제 주소로는 보내지 않고 .env 는 바꾸지 않는다.
#
# LLM: 기본은 가짜 Claude 서버(로컬 루프백)라 실제 호출이 없다.
#      DAILY_E2E_REAL_LLM=1 이면 실제 Claude 를 부른다. 키는 셸 환경변수 ANTHROPIC_API_KEY 에서만 읽고 출력하지 않는다.
#
# 사용법: scripts/manual-daily-briefing.sh
# 환경변수:
#   DAILY_E2E_REAL_LLM   1 이면 실제 Claude(기본 가짜)
#   DAILY_E2E_ENV_FILE   서비스 DB 계정(MARKET_/NEWS_/BRIEFING_DB_USER·PASSWORD)을 읽을 파일(기본 저장소 .env)
#   DAILY_E2E_SMTP_SINK  1 이면 가짜 SMTP 수신 서버로 보낸다(기본 드라이런)
#   DAILY_E2E_OUT_DIR    판 본문을 쓸 폴더(기본 임시 폴더, 저장소 안이면 거절)
#   JAVA_HOME_21         JDK 21
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 1

RUN_ID="$(date +%Y%m%d%H%M%S)_$$"
DB_NAME="daily_e2e_${RUN_ID}"
DB_NAME_PATTERN='^daily_e2e_[0-9]{14}_[0-9]+$'
KAFKA_NAME="daily-e2e-kafka-${RUN_ID//_/-}"
KAFKA_LABEL="stock-reading.daily-e2e"
KAFKA_IMAGE="apache/kafka:4.2.2"
PG_CONTAINER="stock-postgres"
ENV_FILE="${DAILY_E2E_ENV_FILE:-$ROOT/.env}"
PY="$ROOT/services/briefing/.venv/bin/python"
REAL_LLM="${DAILY_E2E_REAL_LLM:-0}"
OUT_DIR="${DAILY_E2E_OUT_DIR:-${TMPDIR:-/tmp}}"
RECIPIENT="daily-e2e@example.com"

TMP=""
KAFKA_STARTED=0
DB_CREATED=0
MARKET_PID="" NEWS_PID="" NOTIFIER_PID="" BRIEFING_PID="" FAKE_LLM_PID="" SINK_PID=""
SMTP_SINK="${DAILY_E2E_SMTP_SINK:-0}"
MARKET_JAR="" NEWS_JAR="" NOTIFIER_JAR=""

fail() { echo "실패: $*" >&2; exit 1; }

dotenv_value() {
  grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d "\"'\r"
}

admin_sql() {
  # 관리자 작업은 컨테이너 안 로컬 접속으로 한다(비밀번호를 다루지 않음). 대상 데이터베이스는 인자로 받는다.
  docker exec -i "$PG_CONTAINER" sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"' sh "$1"
}

wait_for_log() {
  # 로그에 정규식이 나올 때까지 기다린다. 로그 내용은 출력하지 않는다.
  local file=$1 pattern=$2 pid=$3 limit=$4 waited=0
  until grep -qE -- "$pattern" "$file" 2>/dev/null; do
    kill -0 "$pid" 2>/dev/null || return 2
    [ "$waited" -ge "$limit" ] && return 1
    sleep 1; waited=$((waited + 1))
  done
}

wait_ready() {  # 이름, 로그 파일, 정규식, pid, 제한 초
  echo "$1 준비 대기"
  wait_for_log "$2" "$3" "$4" "$5"
  case $? in
    0) ;;
    2) fail "$1 이 시작 중에 끝났습니다(로그: $2)" ;;
    *) fail "$1 이 $5초 안에 준비되지 않았습니다(로그: $2)" ;;
  esac
}

# ---------------------------------------------------------------- 정리

stop_pid() {
  # 저장한 pid 만 끈다. 명령줄에 이 실행의 표식(jar 경로나 임시 폴더)이 있을 때만 끈다.
  local pid=$1 mark=$2 name=$3 cmd waited=0
  [ -n "$pid" ] || return 0
  kill -0 "$pid" 2>/dev/null || return 0
  cmd=$(ps -p "$pid" -o command= 2>/dev/null || true)
  case "$cmd" in
    *"$mark"*) ;;
    *) echo "정리: pid $pid 는 이 실행이 띄운 $name 이 아니어서 두었습니다"; return 0 ;;
  esac
  kill "$pid" 2>/dev/null
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 10 ]; do sleep 1; waited=$((waited + 1)); done
  kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
  if kill -0 "$pid" 2>/dev/null; then echo "정리 실패: $name(pid $pid)이 남았습니다"; return 1; fi
  echo "정리: $name 종료"
}

cleanup() {
  trap - EXIT INT TERM
  local bad=0 label
  stop_pid "$NOTIFIER_PID" "$NOTIFIER_JAR" notifier || bad=1
  stop_pid "$SINK_PID" "$TMP/smtp-sink" "가짜 SMTP 서버" || bad=1
  stop_pid "$BRIEFING_PID" "$TMP/briefing" briefing || bad=1
  stop_pid "$FAKE_LLM_PID" "$TMP/fake-llm" "가짜 LLM" || bad=1
  stop_pid "$NEWS_PID" "$NEWS_JAR" news || bad=1
  stop_pid "$MARKET_PID" "$MARKET_JAR" market || bad=1
  if [ "$KAFKA_STARTED" = 1 ]; then
    label=$(docker inspect -f "{{index .Config.Labels \"$KAFKA_LABEL\"}}" "$KAFKA_NAME" 2>/dev/null || true)
    if [ "$label" = "$RUN_ID" ] && docker rm -f "$KAFKA_NAME" >/dev/null 2>&1; then
      echo "정리: Kafka 컨테이너 $KAFKA_NAME 삭제"
    elif [ -n "$label" ]; then
      echo "정리 실패: Kafka 컨테이너가 남았습니다. docker rm -f $KAFKA_NAME"; bad=1
    fi
  fi
  if [ "$DB_CREATED" = 1 ]; then
    if [[ "$DB_NAME" =~ $DB_NAME_PATTERN ]] \
        && printf 'DROP DATABASE IF EXISTS "%s" WITH (FORCE);\n' "$DB_NAME" | admin_sql postgres >/dev/null; then
      echo "정리: 데이터베이스 $DB_NAME 삭제"
    else
      echo "정리 실패: 데이터베이스 $DB_NAME 이 남았습니다"; bad=1
    fi
  fi
  case "$TMP" in */daily-e2e.*) rm -rf "$TMP" ;; esac
  [ "$bad" = 0 ] || echo "정리하지 못한 것이 있습니다(위 안내를 보고 직접 지우세요)"
}
trap cleanup EXIT
trap 'echo "중단됨: 이 실행이 만든 것을 정리합니다"; exit 130' INT TERM

# ---------------------------------------------------------------- 준비

[ -x "$PY" ] || fail "브리핑 venv 가 없습니다(services/briefing/.venv, docs/local-development.md 참고)"
[ -n "${JAVA_HOME_21:-}" ] && [ -x "$JAVA_HOME_21/bin/java" ] || fail "JAVA_HOME_21 이 JDK 21 을 가리키지 않습니다"
[ -f "$ENV_FILE" ] || fail "서비스 DB 계정을 읽을 파일이 없습니다($ENV_FILE)"
OUT_DIR=$(cd "$OUT_DIR" 2>/dev/null && pwd -P) || fail "결과 폴더가 없습니다"
case "$OUT_DIR/" in "$(cd "$ROOT" && pwd -P)/"*) fail "결과 폴더는 저장소 밖이어야 합니다" ;; esac
RESULT_FILE="$OUT_DIR/daily-e2e-${RUN_ID}.txt"
if [ "$REAL_LLM" = 1 ]; then
  [ -n "${ANTHROPIC_API_KEY:-}" ] || fail "DAILY_E2E_REAL_LLM=1 인데 셸 환경변수 ANTHROPIC_API_KEY 가 없습니다"
fi
docker info >/dev/null 2>&1 || fail "Docker 에 연결할 수 없습니다"
[ "$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null)" = true ] || fail "$PG_CONTAINER 가 켜져 있지 않습니다"

for key in MARKET_DB_USER MARKET_DB_PASSWORD NEWS_DB_USER NEWS_DB_PASSWORD BRIEFING_DB_USER BRIEFING_DB_PASSWORD; do
  [ -n "$(dotenv_value "$key")" ] || fail "$ENV_FILE 에 $key 가 없습니다"
done
M_USER=$(dotenv_value MARKET_DB_USER); M_PW=$(dotenv_value MARKET_DB_PASSWORD)
N_USER=$(dotenv_value NEWS_DB_USER); N_PW=$(dotenv_value NEWS_DB_PASSWORD)
B_USER=$(dotenv_value BRIEFING_DB_USER); B_PW=$(dotenv_value BRIEFING_DB_PASSWORD)
for role in "$M_USER" "$N_USER" "$B_USER"; do
  # SQL 에 그대로 넣으므로 이름 형식을 먼저 확인한다.
  [[ "$role" =~ ^[a-z_][a-z0-9_]*$ ]] || fail "서비스 계정 이름 형식이 맞지 않습니다"
done

TMP=$(mktemp -d "${TMPDIR:-/tmp}/daily-e2e.XXXXXX") || exit 1
mkdir -p "$TMP/market" "$TMP/news" "$TMP/notifier" "$TMP/briefing" "$TMP/fake-llm" "$TMP/smtp-sink"
MAIL_DIR="$OUT_DIR/mail-${RUN_ID}"

echo "bootJar 빌드(market, news, notifier)"
JAVA_HOME="$JAVA_HOME_21" ./gradlew -q :services:market:bootJar :services:news:bootJar :services:notifier:bootJar \
  || fail "bootJar 빌드 실패"
jar_of() { ls "services/$1/build/libs/"*.jar 2>/dev/null | grep -v -- '-plain' | head -1 | sed "s|^|$ROOT/|"; }
MARKET_JAR=$(jar_of market); NEWS_JAR=$(jar_of news); NOTIFIER_JAR=$(jar_of notifier)
[ -n "$MARKET_JAR" ] && [ -n "$NEWS_JAR" ] && [ -n "$NOTIFIER_JAR" ] || fail "bootJar 를 찾지 못했습니다"

PORT=$("$PY" -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
BOOTSTRAP="localhost:$PORT"
echo "일회용 Kafka 기동: $KAFKA_NAME (127.0.0.1:$PORT)"
KAFKA_STARTED=1
docker run -d --name "$KAFKA_NAME" --label "$KAFKA_LABEL=$RUN_ID" -p "127.0.0.1:$PORT:$PORT" \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e "KAFKA_LISTENERS=PLAINTEXT://:$PORT,CONTROLLER://:9093" \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$PORT" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  "$KAFKA_IMAGE" >/dev/null || fail "일회용 Kafka 를 띄우지 못했습니다"
waited=0
until docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
  [ "$waited" -ge 60 ] && fail "일회용 Kafka 가 60초 안에 준비되지 않았습니다"
  sleep 2; waited=$((waited + 2))
done

echo "데이터베이스 $DB_NAME 생성(market·news·briefing 스키마)"
printf 'CREATE DATABASE "%s";\n' "$DB_NAME" | admin_sql postgres >/dev/null || fail "데이터베이스를 만들지 못했습니다"
DB_CREATED=1
admin_sql "$DB_NAME" >/dev/null <<SQL || fail "서비스 스키마를 만들지 못했습니다"
CREATE SCHEMA market AUTHORIZATION "$M_USER";
CREATE SCHEMA news AUTHORIZATION "$N_USER";
CREATE SCHEMA briefing AUTHORIZATION "$B_USER";
SQL
JDBC_URL="jdbc:postgresql://127.0.0.1:5432/$DB_NAME"

# ---------------------------------------------------------------- 서비스

echo "market 기동"
(
  export SPRING_DATASOURCE_URL="$JDBC_URL?currentSchema=market" SPRING_DATASOURCE_USERNAME="$M_USER" SPRING_DATASOURCE_PASSWORD="$M_PW"
  export SPRING_KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP"
  # 가짜 대응표: TEST01 에 미국 종목 두 개, TEST02 는 미국 종목 없음. 전일 종가 기준 감지는 끈다(시세를 넣지 않음).
  export ETF_US_HOLDINGS="$("$PY" -c 'import sys; sys.path.insert(0, "scripts"); import daily_e2e; print(daily_e2e.holdings_setting())')"
  # 같은 값을 보유 종목 파일로도 준다. 파일의 미국 구성 종목이 ETF_US_HOLDINGS 와 어긋나면 시세처리기가 시작하지 않는다.
  "$PY" -c 'import sys; sys.path.insert(0, "scripts"); import daily_e2e; print(daily_e2e.holdings_file_json())' > "$TMP/holdings.json"
  export HOLDINGS_FILE="$TMP/holdings.json"
  export MARKET_PREV_CLOSE_FIELD=
  cd "$TMP/market" && exec "$JAVA_HOME_21/bin/java" -jar "$MARKET_JAR"
) > "$TMP/market.log" 2>&1 &
MARKET_PID=$!

echo "news 기동"
(
  export SPRING_DATASOURCE_URL="$JDBC_URL?currentSchema=news" SPRING_DATASOURCE_USERNAME="$N_USER" SPRING_DATASOURCE_PASSWORD="$N_PW"
  export SPRING_KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP"
  cd "$TMP/news" && exec "$JAVA_HOME_21/bin/java" -jar "$NEWS_JAR"
) > "$TMP/news.log" 2>&1 &
NEWS_PID=$!

if [ "$SMTP_SINK" = 1 ]; then
  echo "가짜 SMTP 수신 서버 기동"
  "$PY" "$ROOT/scripts/daily_e2e.py" smtp-sink "$TMP/smtp-sink/port" "$MAIL_DIR" > "$TMP/smtp-sink.log" 2>&1 &
  SINK_PID=$!
  disown "$SINK_PID"
  waited=0
  until [ -s "$TMP/smtp-sink/port" ]; do
    kill -0 "$SINK_PID" 2>/dev/null || fail "가짜 SMTP 서버가 시작 중에 끝났습니다"
    [ "$waited" -ge 10 ] && fail "가짜 SMTP 서버가 10초 안에 준비되지 않았습니다"
    sleep 1; waited=$((waited + 1))
  done
  echo "notifier 기동(가짜 SMTP 로만 발송)"
else
  echo "notifier 기동(드라이런)"
fi
(
  export SPRING_KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP"
  export NOTIFIER_RECIPIENT="$RECIPIENT" NOTIFIER_SENDER="$RECIPIENT"
  unset SMTP_HOST SMTP_PORT SMTP_USERNAME SMTP_PASSWORD
  if [ "$SMTP_SINK" = 1 ]; then
    # 이 인스턴스에만 발송을 켠다. 목적지는 루프백 수신 서버뿐이라 암호화·인증은 끈다.
    # 호스트·포트는 환경변수보다 먼저 쓰이는 SPRING_APPLICATION_JSON 으로 넣고, 셸에 남은 메일 설정은 지운다.
    unset SPRING_MAIL_HOST SPRING_MAIL_PORT SPRING_MAIL_USERNAME SPRING_MAIL_PASSWORD
    export NOTIFIER_SEND_ENABLED=true
    SPRING_APPLICATION_JSON="$("$PY" "$ROOT/scripts/daily_e2e.py" smtp-json "$(cat "$TMP/smtp-sink/port")")" \
      || fail "가짜 SMTP 설정을 만들지 못했습니다"
    export SPRING_APPLICATION_JSON
  else
    # 발송 스위치는 끈 채로 둔다. SMTP 설정은 넘기지 않는다.
    export NOTIFIER_SEND_ENABLED=false
  fi
  export LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
  cd "$TMP/notifier" && exec "$JAVA_HOME_21/bin/java" -jar "$NOTIFIER_JAR"
) > "$TMP/notifier.log" 2>&1 &
NOTIFIER_PID=$!

if [ "$REAL_LLM" = 1 ]; then
  echo "LLM: 실제 Claude"
else
  echo "LLM: 가짜 Claude 서버 기동"
  "$PY" "$ROOT/scripts/daily_e2e.py" fake-llm "$TMP/fake-llm/port" > "$TMP/fake-llm.log" 2>&1 &
  FAKE_LLM_PID=$!
  # 정리할 때 끈 것을 셸이 "Terminated" 로 알리지 않게 한다. 끄는 것은 저장한 pid 로 한다.
  disown "$FAKE_LLM_PID"
  waited=0
  until [ -s "$TMP/fake-llm/port" ]; do
    kill -0 "$FAKE_LLM_PID" 2>/dev/null || fail "가짜 LLM 서버가 시작 중에 끝났습니다"
    [ "$waited" -ge 10 ] && fail "가짜 LLM 서버가 10초 안에 준비되지 않았습니다"
    sleep 1; waited=$((waited + 1))
  done
  FAKE_LLM_URL="http://127.0.0.1:$(cat "$TMP/fake-llm/port")"
fi

wait_ready market "$TMP/market.log" "groupId=market-insight\] Adding newly assigned partitions" "$MARKET_PID" 120
wait_ready market "$TMP/market.log" "groupId=market\] Adding newly assigned partitions" "$MARKET_PID" 60
wait_ready news "$TMP/news.log" "groupId=news-filings\] Adding newly assigned partitions" "$NEWS_PID" 120
wait_ready news "$TMP/news.log" "groupId=news\] Adding newly assigned partitions" "$NEWS_PID" 60
# notifier 는 처음 시작할 때 latest 부터 읽으므로 판이 나오기 전에 파티션을 받아 두어야 한다.
wait_ready notifier "$TMP/notifier.log" "groupId=notifier\] Adding newly assigned partitions:.*briefing\.daily\.generated" "$NOTIFIER_PID" 120

echo "briefing 기동"
(
  export KAFKA_BOOTSTRAP="$BOOTSTRAP" POSTGRES_DB="$DB_NAME" BRIEFING_DB_USER="$B_USER" BRIEFING_DB_PASSWORD="$B_PW"
  export PYTHONPATH="$ROOT/services/briefing/src" PYTHONUNBUFFERED=1
  if [ "$REAL_LLM" = 1 ]; then
    unset ANTHROPIC_BASE_URL
  else
    # 공식 SDK 는 ANTHROPIC_BASE_URL 로 주소를 바꿀 수 있다. 키는 가짜 서버가 보지 않는 시험용 값이다.
    export ANTHROPIC_BASE_URL="$FAKE_LLM_URL" ANTHROPIC_API_KEY="daily-e2e-fake-key"
  fi
  # 캐시 위치를 임시 폴더로 두어 명령줄에 이 실행의 표식이 남게 한다(정리할 때 확인).
  cd "$TMP/briefing" && exec "$PY" -X "pycache_prefix=$TMP/briefing/pycache" -m briefing.main
) > "$TMP/briefing.log" 2>&1 &
BRIEFING_PID=$!
wait_ready briefing "$TMP/briefing.log" "briefing started:" "$BRIEFING_PID" 60
grep -qF "daily briefings will be rule-based" "$TMP/briefing.log" && fail "briefing 이 LLM 없이(규칙 기반으로) 시작됐습니다"

# 판 tick 그룹은 latest 부터 읽으므로 파티션을 받기 전에 보낸 tick 은 놓친다. 두 그룹 모두 받을 때까지 기다린다.
for group in briefing-daily-morning briefing-daily-post-close; do
  waited=0
  until docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" \
      --describe --group "$group" --members 2>/dev/null | grep -qE '[[:space:]]1[[:space:]]*$'; do
    [ "$waited" -ge 60 ] && fail "브리핑 $group 그룹이 60초 안에 파티션을 받지 못했습니다"
    sleep 2; waited=$((waited + 2))
  done
done

# ---------------------------------------------------------------- 흐름

NOW=$(TZ=Asia/Seoul date +%Y-%m-%dT%H:%M:%S+09:00)
export DAILY_E2E_KAFKA="$BOOTSTRAP" DAILY_E2E_RUN_ID="$RUN_ID" DAILY_E2E_NOW="$NOW" DAILY_E2E_RESULT="$RESULT_FILE"

echo "가짜 수집 자료 넣기"
"$PY" scripts/daily_e2e.py seed || fail "수집 자료를 넣지 못했습니다"
count_rows() { echo "$1" | admin_sql "$DB_NAME" 2>/dev/null; }
waited=0
until [ "$(count_rows 'SELECT (SELECT count(*) FROM market.daily_price) + (SELECT count(*) FROM market.world_index)
    + (SELECT count(*) FROM market.exchange_rate_summary) + (SELECT count(*) FROM market.market_summary)
    + (SELECT count(*) FROM market.overseas_daily_price) + (SELECT count(*) FROM news.filing);')" = 9 ]; do
  [ "$waited" -ge 60 ] && fail "수집 자료 9건이 60초 안에 저장되지 않았습니다"
  sleep 2; waited=$((waited + 2))
done
echo "저장 확인: 시세처리기 8건, 뉴스(공시) 1건"

echo "아침 판·마감 후 판 시작 신호 넣기(예정 시각 $NOW)"
"$PY" scripts/daily_e2e.py ticks || fail "시작 신호를 넣지 못했습니다"

# 실제 Claude 는 근거 약한 문장마다 재검색을 요청하고, 이 실행의 news 는 검색하지 않으므로 판이 재검색 전체 상한
# (CoreSettings.research_total_limit, 15분)까지 기다렸다가 나간다. 그보다 길게 기다린다.
if [ "$REAL_LLM" = 1 ]; then MAIL_WAIT=1020; else MAIL_WAIT=120; fi
echo "발송기 로그 대기(최대 ${MAIL_WAIT}초)"
if [ "$SMTP_SINK" = 1 ]; then SENT_PATTERN="email sent to="; else SENT_PATTERN="DRY-RUN (not sent)"; fi
waited=0
until [ "$(grep -c -- "$SENT_PATTERN" "$TMP/notifier.log")" -ge 2 ]; do
  [ "$waited" -ge "$MAIL_WAIT" ] && break
  sleep 2; waited=$((waited + 2))
done

# ---------------------------------------------------------------- 결과

status=0
echo "--- briefing.daily.generated"
"$PY" scripts/daily_e2e.py check || status=1
echo "--- 브리핑 로그"
grep -cE "daily (morning|post-close) started" "$TMP/briefing.log" | sed 's/^/판 시작: /'
grep -c "published daily briefing" "$TMP/briefing.log" | sed 's/^/판 발행: /'
grep -c "daily LLM attempt" "$TMP/briefing.log" | sed 's/^/LLM 거부·실패 시도: /'
# Claude SDK 의 HTTP 클라이언트가 요청마다 남기는 줄을 센다. 다시 보낸 요청도 한 번씩 들어간다.
llm_requests=$(grep -cE "HTTP Request: POST [^ ]*/v1/messages" "$TMP/briefing.log")
if [ "$REAL_LLM" = 1 ]; then
  echo "실제 Claude 호출: $llm_requests"
else
  calls=$(curl -s "$FAKE_LLM_URL/__stats" | "$PY" -c 'import json,sys; print(json.load(sys.stdin)["calls"])')
  echo "가짜 LLM 호출: $calls (브리핑 로그의 요청 줄 $llm_requests)"
fi
if [ "$SMTP_SINK" = 1 ]; then
  echo "--- 발송기(가짜 SMTP 수신 서버)"
  sent=$(grep -c "email sent to=" "$TMP/notifier.log")
  echo "발송 로그: $sent"
  received=$(ls "$MAIL_DIR"/*.eml 2>/dev/null | wc -l | tr -d ' ')
  echo "수신 서버가 받은 메일: $received"
  grep -qF "$RECIPIENT" "$TMP/notifier.log" && { echo "주소가 마스킹되지 않은 줄이 있습니다"; status=1; }
  grep -qF "DRY-RUN (not sent)" "$TMP/notifier.log" && { echo "드라이런 로그가 섞여 있습니다"; status=1; }
  [ "$sent" -ge 2 ] && [ "$received" -ge 2 ] || status=1
  echo "받은 메일 폴더: $MAIL_DIR"
else
  echo "--- 발송기 드라이런"
  dry=$(grep -c "DRY-RUN (not sent)" "$TMP/notifier.log")
  echo "드라이런 메일: $dry"
  # HTML 이 있는 메일은 제목 뒤에 HTML 글자 수와 그림 수·크기가 붙는다.
  grep -oE 'DRY-RUN \(not sent\) to=[^ ]* subject="[^"]*"( html=[0-9]+chars images=[0-9]+\[[^]]*\])?' \
    "$TMP/notifier.log" || true
  grep -qF "$RECIPIENT" "$TMP/notifier.log" && { echo "주소가 마스킹되지 않은 줄이 있습니다"; status=1; }
  grep -qF "email sent to=" "$TMP/notifier.log" && { echo "실제 발송 로그가 있습니다"; status=1; }
  [ "$dry" -ge 2 ] || status=1
fi
echo "판 본문: $RESULT_FILE"
if [ "$status" = 0 ]; then echo "결과: 통과"; else
  echo "결과: 실패 — 서비스 로그 끝부분"
  for name in briefing market news notifier; do echo "== $name"; tail -n 20 "$TMP/$name.log"; done
fi
exit "$status"
