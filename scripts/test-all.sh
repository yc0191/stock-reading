#!/usr/bin/env bash
# 전체 실행 스크립트. 단위·가상 테스트와 실제 DB 테스트를 항상 돌리고, --live 일 때만 시세 흐름 실제 테스트를 돌린 뒤
# 테스트 지도(docs/test-map)의 기능별 '돌았음 / 건너뜀(이유) / 실패' 표를 출력한다. 하나라도 실패하면 1로 끝난다.
#
# 사용법: scripts/test-all.sh [--live] [--plan]
#   --live  실제 시세 서버를 부른다(토큰 1 + 시세 1). 사람이 직접 결정해서만 쓴다.
#   --plan  아무것도 띄우지 않고 실행 순서만 보여 준다.
#
# 실제 DB 테스트는 개발용 stock-postgres 안에 이 실행만의 stock_test_run_<시각>_<pid> 데이터베이스를 만들어 쓰고 지운다.
# 꺼져 있던 stock-postgres 는 켜고 끝나도 끄지 않는다. 서비스 계정은 만들거나 바꾸지 않는다.
# 서비스 계정 비밀번호와 시세 키는 .env 에서 필요한 키만 골라 읽어 자식 프로세스 환경변수로만 넘기고 출력하지 않는다.
#
# 환경변수(모두 선택):
#   TEST_ALL_ENV_FILE  .env 위치(기본 저장소 루트의 .env)
#   TEST_ALL_PYTHON    브리핑 venv 파이썬(기본 services/briefing/.venv/bin/python)
#   JAVA_HOME_21       JDK 21 위치(없으면 /usr/libexec/java_home -v 21 로 찾음)
#   --live 전용: BROKER_BASE_URL, BROKER_APP_KEY, BROKER_APP_SECRET, TEST_ALL_LIVE_SYMBOL(6자리)
#               실행 환경변수를 먼저 보고 없으면 .env 에서 읽는다.
set -uo pipefail

usage() {
  sed -n '5,7p' "$0" | sed 's/^# \{0,1\}//'
}

LIVE=0
PLAN=0
for arg in "$@"; do
  case "$arg" in
    --live) LIVE=1 ;;
    --plan) PLAN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "알 수 없는 옵션: $arg" >&2; usage >&2; exit 64 ;;
  esac
done

LIVE_SKIP_REASON="실제 호출 옵션 없음"

if [ "$PLAN" = 1 ]; then
  echo "실행 순서 (--plan: 아무것도 띄우지 않음)"
  echo "1. Java 전체: ./gradlew cleanTest test --no-build-cache"
  echo "2. 브리핑: pytest services/briefing"
  echo "3. 설정 로더·지도 검사: pytest -c scripts/pytest.ini scripts/tests"
  echo "4. 실제 DB 준비: stock-postgres 확인(꺼져 있으면 켬), stock_test_run_<시각>_<pid> 데이터베이스와 market·news·briefing 스키마 생성"
  echo "5. 실제 DB 테스트: 지도에서 고른 market·news·브리핑 실제 DB 테스트"
  if [ "$LIVE" = 1 ]; then
    echo "6. 시세 흐름 실제: 일회용 Kafka, market(테스트 전용 데이터베이스), collector(실제 시세, 종목 1개), tick 1건, 저장 확인"
  else
    echo "6. 시세 흐름 실제: 건너뜀($LIVE_SKIP_REASON)"
  fi
  echo "7. 정리: 이 실행이 만든 데이터베이스·컨테이너·프로세스·임시 폴더만 지움"
  echo "8. docs/test-map 기능별 결과 표 출력"
  exit 0
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 1

RUN_ID="$(date +%Y%m%d%H%M%S)_$$"
DB_NAME="stock_test_run_${RUN_ID}"
DB_NAME_PATTERN='^stock_test_run_[0-9]{14}_[0-9]+$'
KAFKA_IMAGE="apache/kafka:4.2.2"
KAFKA_NAME="stock-test-kafka-${RUN_ID//_/-}"
KAFKA_LABEL="stock-reading.test-all"
PG_CONTAINER="stock-postgres"
ENV_FILE="${TEST_ALL_ENV_FILE:-$ROOT/.env}"
PY="${TEST_ALL_PYTHON:-$ROOT/services/briefing/.venv/bin/python}"
LOCK_DIR="$ROOT/build/test-all.lock"
JAVA_SERVICES="collector market news notifier"

# 바깥에서 들어온 켜는 변수가 기본 단계에 섞이면 엉뚱한 데이터베이스로 실제 DB 테스트가 돈다. 필요한 단계에서만 넣는다.
unset MARKET_IT_DB_URL MARKET_IT_DB_USER MARKET_IT_DB_PASSWORD NEWS_IT_DB_URL NEWS_IT_DB_USER NEWS_IT_DB_PASSWORD \
  BRIEFING_IT_DB_URL BRIEFING_IT_DB_USER BRIEFING_IT_DB_PASSWORD \
  QUOTE_FLOW_LIVE_DB_URL QUOTE_FLOW_LIVE_DB_USER QUOTE_FLOW_LIVE_DB_PASSWORD

TMP=""
LOCKED=0
DB_CREATED=0
KAFKA_STARTED=0
MARKET_PID=""
COLLECTOR_PID=""
MARKET_JAR=""
COLLECTOR_JAR=""
CLEANED=0

log() { printf '\n== %s\n' "$*"; }

stage() {
  # 키, 이름, 상태(ok|fail|skip), 설명, 초, 결과 폴더(results 기준)
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "${6:-}" >> "$TMP/results/stages.tsv"
}

dotenv_value() {
  [ -f "$ENV_FILE" ] || return 0
  grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d "\"'\r"
}

# 실행 환경변수를 먼저 보고, 없으면 .env 에서 읽는다.
env_or_dotenv() {
  local value
  eval "value=\${$1:-}"
  if [ -n "$value" ]; then printf '%s' "$value"; else dotenv_value "$1"; fi
}

admin_sql() {
  # 관리자 작업은 컨테이너 안 로컬 접속으로 한다(비밀번호를 다루지 않음). 개발용 데이터베이스가 아닌 postgres 에 붙는다.
  docker exec -i "$PG_CONTAINER" sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres'
}

# ---------------------------------------------------------------- 정리

stop_service() {
  local pid=$1 jar=$2 name=$3 cmd waited
  [ -n "$pid" ] || return 0
  kill -0 "$pid" 2>/dev/null || return 0
  cmd=$(ps -p "$pid" -o command= 2>/dev/null || true)
  case "$cmd" in
    *"$jar"*) ;;
    *) echo "정리: pid $pid 는 이 실행이 띄운 $name 이 아니어서 두었습니다"; return 0 ;;
  esac
  kill "$pid" 2>/dev/null
  waited=0
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 10 ]; do sleep 1; waited=$((waited + 1)); done
  if kill -0 "$pid" 2>/dev/null; then kill -9 "$pid" 2>/dev/null; fi
  if kill -0 "$pid" 2>/dev/null; then echo "정리 실패: $name 프로세스(pid $pid)가 남았습니다. kill -9 $pid"; return 1; fi
  echo "정리: $name 프로세스 종료"
}

remove_kafka() {
  [ "$KAFKA_STARTED" = 1 ] || return 0
  local label
  label=$(docker inspect -f "{{index .Config.Labels \"$KAFKA_LABEL\"}}" "$KAFKA_NAME" 2>/dev/null) || { KAFKA_STARTED=0; return 0; }
  if [ "$label" != "$RUN_ID" ]; then
    echo "정리: $KAFKA_NAME 의 라벨이 이 실행과 달라 두었습니다"
    return 1
  fi
  if docker rm -f "$KAFKA_NAME" >/dev/null 2>&1; then
    echo "정리: Kafka 컨테이너 $KAFKA_NAME 삭제"
    KAFKA_STARTED=0
  else
    echo "정리 실패: Kafka 컨테이너가 남았습니다. docker rm -f $KAFKA_NAME"
    return 1
  fi
}

drop_db() {
  [ "$DB_CREATED" = 1 ] || return 0
  if ! [[ "$DB_NAME" =~ $DB_NAME_PATTERN ]]; then
    echo "정리: $DB_NAME 은 이 실행이 만드는 이름 형식이 아니어서 지우지 않았습니다"
    return 1
  fi
  if printf 'DROP DATABASE IF EXISTS "%s" WITH (FORCE);\n' "$DB_NAME" | admin_sql >/dev/null; then
    echo "정리: 데이터베이스 $DB_NAME 삭제"
    DB_CREATED=0
  else
    echo "정리 실패: 데이터베이스가 남았습니다. docker exec -i $PG_CONTAINER psql -U <관리자> -d postgres -c 'DROP DATABASE \"$DB_NAME\" WITH (FORCE)'"
    return 1
  fi
}

# 이 실행이 만든 것(서비스 프로세스, Kafka 컨테이너, 데이터베이스)을 지운다. 여러 번 불러도 같다.
cleanup_resources() {
  local bad=0
  stop_service "$COLLECTOR_PID" "$COLLECTOR_JAR" collector || bad=1
  COLLECTOR_PID=""
  stop_service "$MARKET_PID" "$MARKET_JAR" market || bad=1
  MARKET_PID=""
  remove_kafka || bad=1
  drop_db || bad=1
  return "$bad"
}

remove_tmp() {
  case "$TMP" in
    */test-all.*) rm -rf "$TMP" ;;
  esac
  TMP=""
}

release_lock() {
  if [ "$LOCKED" = 1 ]; then rm -rf "$LOCK_DIR"; LOCKED=0; fi
}

on_exit() {
  trap - EXIT INT TERM
  if [ "$CLEANED" = 0 ]; then
    cleanup_resources || echo "정리하지 못한 것이 있습니다(위 안내를 보고 직접 지우세요)"
  fi
  remove_tmp
  release_lock
}

on_signal() {
  echo "중단됨($1): 이 실행이 만든 것을 정리합니다"
  exit 130
}

trap on_exit EXIT
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

# ---------------------------------------------------------------- 준비

mkdir -p "$ROOT/build"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  other=$(cat "$LOCK_DIR/pid" 2>/dev/null || true)
  if [ -n "$other" ] && kill -0 "$other" 2>/dev/null; then
    echo "이 작업 폴더에서 다른 전체 실행(pid $other)이 돌고 있습니다. 끝난 뒤 다시 실행하세요" >&2
    exit 1
  fi
  # 강제 종료로 남은 잠금이다.
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR" || exit 1
fi
LOCKED=1
echo $$ > "$LOCK_DIR/pid"

TMP=$(mktemp -d "${TMPDIR:-/tmp}/test-all.XXXXXX") || exit 1
mkdir -p "$TMP/results/main/py" "$TMP/results/db/py" "$TMP/results/live/py"
: > "$TMP/results/stages.tsv"
STARTED_AT=$SECONDS

is_jdk21() {
  # java_home -v 21 은 21이 없으면 다른 버전을 돌려주므로 실제 버전을 확인한다.
  [ -n "$1" ] && [ -x "$1/bin/java" ] && "$1/bin/java" -version 2>&1 | head -1 | grep -qE 'version "21[."]'
}

JAVA_OK=1
if [ -z "${JAVA_HOME_21:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME_21=$(/usr/libexec/java_home -v 21 2>/dev/null || true)
fi
if is_jdk21 "${JAVA_HOME_21:-}"; then
  export JAVA_HOME_21 JAVA_HOME="$JAVA_HOME_21"
else
  JAVA_OK=0
fi
PY_OK=1
[ -x "$PY" ] || PY_OK=0

collect_java() {
  local phase=$1 services=$2 svc src
  for svc in $services; do
    src="services/$svc/build/test-results/test"
    if [ -d "$src" ]; then
      mkdir -p "$TMP/results/$phase/java/$svc"
      cp "$src"/*.xml "$TMP/results/$phase/java/$svc/" 2>/dev/null || true
    fi
  done
}

# ---------------------------------------------------------------- 1~3. 단위·가상

log "1. Java 전체"
t=$SECONDS
if [ "$JAVA_OK" = 1 ]; then
  ./gradlew cleanTest test --no-build-cache
  rc=$?
  collect_java main "$JAVA_SERVICES"
  if [ "$rc" = 0 ]; then stage java "Java 전체" ok "" $((SECONDS - t)) main/java
  else stage java "Java 전체" fail "gradlew 종료 코드 $rc" $((SECONDS - t)) main/java; fi
else
  stage java "Java 전체" fail "JDK 21 을 찾지 못함(JAVA_HOME_21 이 없거나 21 이 아님)" 0
fi

log "2. 브리핑"
t=$SECONDS
if [ "$PY_OK" = 1 ]; then
  "$PY" -m pytest services/briefing --junitxml="$TMP/results/main/py/briefing.xml"
  rc=$?
  if [ "$rc" = 0 ]; then stage briefing "브리핑" ok "" $((SECONDS - t)) main/py/briefing.xml
  else stage briefing "브리핑" fail "pytest 종료 코드 $rc" $((SECONDS - t)) main/py/briefing.xml; fi
else
  stage briefing "브리핑" fail "브리핑 venv 파이썬이 없음(TEST_ALL_PYTHON)" 0
fi

log "3. 설정 로더·지도 검사"
t=$SECONDS
if [ "$PY_OK" = 1 ]; then
  "$PY" -m pytest -c scripts/pytest.ini scripts/tests --junitxml="$TMP/results/main/py/scripts.xml"
  rc=$?
  if [ "$rc" = 0 ]; then stage scripts "설정 로더·지도 검사" ok "" $((SECONDS - t)) main/py/scripts.xml
  else stage scripts "설정 로더·지도 검사" fail "pytest 종료 코드 $rc" $((SECONDS - t)) main/py/scripts.xml; fi
else
  stage scripts "설정 로더·지도 검사" fail "브리핑 venv 파이썬이 없음(TEST_ALL_PYTHON)" 0
fi

# ---------------------------------------------------------------- 4. 실제 DB 준비

M_USER="" M_PW="" N_USER="" N_PW="" B_USER="" B_PW=""
DB_FAIL_REASON=""

warn_leftovers() {
  local dbs containers
  dbs=$(echo "SELECT datname FROM pg_database WHERE datname LIKE 'stock\\_test\\_run\\_%' ORDER BY 1;" | admin_sql 2>/dev/null)
  containers=$(docker ps -a --filter "label=$KAFKA_LABEL" --format '{{.Names}}' 2>/dev/null)
  if [ -n "$dbs" ] || [ -n "$containers" ]; then
    echo "경고: 이전 전체 실행이 남긴 것으로 보이는 것이 있습니다(다른 실행이 아직 돌고 있을 수 있어 지우지 않음)"
    for d in $dbs; do echo "  데이터베이스 $d  → docker exec -i $PG_CONTAINER psql -U <관리자> -d postgres -c 'DROP DATABASE \"$d\" WITH (FORCE)'"; done
    for c in $containers; do echo "  컨테이너 $c  → docker rm -f $c"; done
  fi
}

prepare_db() {
  local running waited key role found
  if ! docker info >/dev/null 2>&1; then DB_FAIL_REASON="Docker 에 연결할 수 없음"; return 1; fi
  if ! running=$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null); then
    DB_FAIL_REASON="$PG_CONTAINER 컨테이너가 없음(infra/compose 로 먼저 만드세요)"; return 1
  fi
  if [ "$running" != true ]; then
    echo "$PG_CONTAINER 가 꺼져 있어 켭니다(끝나도 끄지 않음)"
    docker start "$PG_CONTAINER" >/dev/null || { DB_FAIL_REASON="$PG_CONTAINER 를 켜지 못함"; return 1; }
  fi
  waited=0
  until docker exec "$PG_CONTAINER" sh -c 'pg_isready -q -U "$POSTGRES_USER"' >/dev/null 2>&1; do
    if [ "$waited" -ge 60 ]; then DB_FAIL_REASON="$PG_CONTAINER 가 60초 안에 준비되지 않음"; return 1; fi
    sleep 1; waited=$((waited + 1))
  done
  if [ ! -f "$ENV_FILE" ]; then DB_FAIL_REASON=".env 가 없음(TEST_ALL_ENV_FILE)"; return 1; fi
  for key in MARKET_DB_USER MARKET_DB_PASSWORD NEWS_DB_USER NEWS_DB_PASSWORD BRIEFING_DB_USER BRIEFING_DB_PASSWORD; do
    if [ -z "$(dotenv_value "$key")" ]; then DB_FAIL_REASON=".env 에 $key 가 없음"; return 1; fi
  done
  M_USER=$(dotenv_value MARKET_DB_USER); M_PW=$(dotenv_value MARKET_DB_PASSWORD)
  N_USER=$(dotenv_value NEWS_DB_USER); N_PW=$(dotenv_value NEWS_DB_PASSWORD)
  B_USER=$(dotenv_value BRIEFING_DB_USER); B_PW=$(dotenv_value BRIEFING_DB_PASSWORD)
  for role in "$M_USER" "$N_USER" "$B_USER"; do
    # SQL 에 그대로 넣으므로 이름 형식을 먼저 확인한다.
    if ! [[ "$role" =~ ^[a-z_][a-z0-9_]*$ ]]; then DB_FAIL_REASON=".env 의 서비스 계정 이름 형식이 맞지 않음"; return 1; fi
    found=$(printf "SELECT 1 FROM pg_roles WHERE rolname = '%s';\n" "$role" | admin_sql 2>/dev/null)
    if [ "$found" != 1 ]; then DB_FAIL_REASON="서비스 계정 $role 이 없음(계정은 만들지 않음, docs/local-development.md 참고)"; return 1; fi
  done
  warn_leftovers
  if ! printf 'CREATE DATABASE "%s";\n' "$DB_NAME" | admin_sql >/dev/null; then
    DB_FAIL_REASON="데이터베이스 $DB_NAME 를 만들지 못함"; return 1
  fi
  DB_CREATED=1
  if ! docker exec -i "$PG_CONTAINER" sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"' sh "$DB_NAME" >/dev/null <<SQL
CREATE SCHEMA market AUTHORIZATION "$M_USER";
CREATE SCHEMA news AUTHORIZATION "$N_USER";
CREATE SCHEMA briefing AUTHORIZATION "$B_USER";
SQL
  then
    DB_FAIL_REASON="서비스 스키마를 만들지 못함"; return 1
  fi
}

log "4. 실제 DB 준비"
t=$SECONDS
if prepare_db; then
  DB_READY=1
  stage dbprep "실제 DB 준비" ok "$DB_NAME" $((SECONDS - t))
else
  DB_READY=0
  echo "실제 DB 준비 실패: $DB_FAIL_REASON"
  stage dbprep "실제 DB 준비" fail "$DB_FAIL_REASON" $((SECONDS - t))
fi

# ---------------------------------------------------------------- 5. 실제 DB 테스트

JDBC_URL="jdbc:postgresql://127.0.0.1:5432/$DB_NAME"
PG_URL="postgresql://127.0.0.1:5432/$DB_NAME"

run_db_tests() {
  local line group names name gradle_args="" java_groups="" briefing_files="" rc=0 path
  # 지도(docs/test-map)에서 켜는 조건이 *_IT_DB_* 인 테스트를 고른다. 새 실제 DB 테스트를 지도에 올리면 여기에 들어온다.
  while read -r group names; do
    [ -n "$group" ] || continue
    case "$group" in
      market|news)
        java_groups="$java_groups $group"
        gradle_args="$gradle_args :services:$group:cleanTest :services:$group:test"
        for name in $names; do gradle_args="$gradle_args --tests $name"; done ;;
      briefing)
        for name in $names; do
          path=$(find services/briefing/tests -name "$name" -o -name "$name.py" | head -1)
          [ -n "$path" ] && briefing_files="$briefing_files $path"
        done ;;
      *) echo "실제 DB 테스트를 돌리는 방법이 없는 지도: $group"; rc=1 ;;
    esac
  done < <(python3 scripts/run_report.py db-tests)
  if [ -z "$java_groups$briefing_files" ]; then echo "지도에서 실제 DB 테스트를 찾지 못했습니다"; return 1; fi
  if [ -n "$java_groups" ]; then
    if [ "$JAVA_OK" = 1 ]; then
      # shellcheck disable=SC2086
      (
        export MARKET_IT_DB_URL="$JDBC_URL" MARKET_IT_DB_USER="$M_USER" MARKET_IT_DB_PASSWORD="$M_PW"
        export NEWS_IT_DB_URL="$JDBC_URL" NEWS_IT_DB_USER="$N_USER" NEWS_IT_DB_PASSWORD="$N_PW"
        ./gradlew $gradle_args --no-build-cache
      ) || rc=1
      collect_java db "$java_groups"
    else
      rc=1
    fi
  fi
  if [ -n "$briefing_files" ]; then
    if [ "$PY_OK" = 1 ]; then
      # shellcheck disable=SC2086
      (
        export BRIEFING_IT_DB_URL="$PG_URL" BRIEFING_IT_DB_USER="$B_USER" BRIEFING_IT_DB_PASSWORD="$B_PW"
        "$PY" -m pytest $briefing_files --junitxml="$TMP/results/db/py/briefing.xml"
      ) || rc=1
    else
      rc=1
    fi
  fi
  return "$rc"
}

log "5. 실제 DB 테스트"
t=$SECONDS
if [ "$DB_READY" = 1 ]; then
  if run_db_tests; then stage db "실제 DB 테스트" ok "" $((SECONDS - t)) db
  else stage db "실제 DB 테스트" fail "" $((SECONDS - t)) db; fi
else
  stage db "실제 DB 테스트" fail "DB 준비 실패로 돌리지 못함" 0
fi

# ---------------------------------------------------------------- 6. 시세 흐름 실제 (--live)

LIVE_FAIL_REASON=""

wait_for_log() {
  # 서비스 로그에 문구가 나올 때까지 기다린다. 로그 내용은 출력하지 않는다(종목 코드가 있을 수 있음).
  local file=$1 pattern=$2 pid=$3 limit=$4 waited=0
  until grep -qF -- "$pattern" "$file" 2>/dev/null; do
    kill -0 "$pid" 2>/dev/null || return 2
    [ "$waited" -ge "$limit" ] && return 1
    sleep 1; waited=$((waited + 1))
  done
}

run_live() {
  local base_url app_key app_secret symbol port waited svc_dir
  base_url=$(env_or_dotenv BROKER_BASE_URL)
  app_key=$(env_or_dotenv BROKER_APP_KEY)
  app_secret=$(env_or_dotenv BROKER_APP_SECRET)
  symbol=$(env_or_dotenv TEST_ALL_LIVE_SYMBOL)
  if [ -z "$base_url" ] || [ -z "$app_key" ] || [ -z "$app_secret" ] || [ -z "$symbol" ]; then
    LIVE_FAIL_REASON="키 없음(BROKER_BASE_URL·BROKER_APP_KEY·BROKER_APP_SECRET·TEST_ALL_LIVE_SYMBOL 중 빈 값)"; return 1
  fi
  if ! [[ "$symbol" =~ ^[0-9]{6}$ ]]; then LIVE_FAIL_REASON="TEST_ALL_LIVE_SYMBOL 이 6자리 숫자가 아님"; return 1; fi
  if [ "$JAVA_OK" = 0 ] || [ "$PY_OK" = 0 ]; then LIVE_FAIL_REASON="JDK 21 또는 브리핑 venv 가 없음"; return 1; fi

  echo "bootJar 빌드"
  ./gradlew -q :services:collector:bootJar :services:market:bootJar || { LIVE_FAIL_REASON="bootJar 빌드 실패"; return 1; }
  MARKET_JAR=$(ls services/market/build/libs/*.jar 2>/dev/null | grep -v -- '-plain' | head -1)
  COLLECTOR_JAR=$(ls services/collector/build/libs/*.jar 2>/dev/null | grep -v -- '-plain' | head -1)
  if [ -z "$MARKET_JAR" ] || [ -z "$COLLECTOR_JAR" ]; then LIVE_FAIL_REASON="bootJar 를 찾지 못함"; return 1; fi
  MARKET_JAR="$ROOT/$MARKET_JAR"
  COLLECTOR_JAR="$ROOT/$COLLECTOR_JAR"

  port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  echo "일회용 Kafka 기동: $KAFKA_NAME (127.0.0.1:$port)"
  KAFKA_STARTED=1
  # 컨테이너 안팎에서 같은 포트를 써서 publish-tick.sh 도 같은 주소로 붙게 한다. 볼륨이 없어 지우면 데이터도 사라진다.
  if ! docker run -d --name "$KAFKA_NAME" --label "$KAFKA_LABEL=$RUN_ID" -p "127.0.0.1:$port:$port" \
      -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
      -e "KAFKA_LISTENERS=PLAINTEXT://:$port,CONTROLLER://:9093" \
      -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$port" \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
      "$KAFKA_IMAGE" >/dev/null; then
    LIVE_FAIL_REASON="일회용 Kafka 를 띄우지 못함"; return 1
  fi
  waited=0
  until docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "localhost:$port" >/dev/null 2>&1; do
    if [ "$waited" -ge 60 ]; then LIVE_FAIL_REASON="일회용 Kafka 가 60초 안에 준비되지 않음"; return 1; fi
    sleep 2; waited=$((waited + 2))
  done

  # 작업 위치를 임시 폴더 안쪽으로 두어 market 이 저장소의 .env 를 읽지 않게 한다.
  svc_dir="$TMP/svc"
  mkdir -p "$svc_dir/market" "$svc_dir/collector"
  echo "market 기동(테스트 전용 데이터베이스)"
  (
    export SPRING_DATASOURCE_URL="$JDBC_URL?currentSchema=market" SPRING_DATASOURCE_USERNAME="$M_USER" SPRING_DATASOURCE_PASSWORD="$M_PW"
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:$port"
    cd "$svc_dir/market" && exec "$JAVA_HOME_21/bin/java" -jar "$MARKET_JAR"
  ) > "$svc_dir/market.log" 2>&1 &
  MARKET_PID=$!
  # Kafka 클라이언트가 그룹에 파티션을 붙였을 때 남기는 INFO 줄이다.
  wait_for_log "$svc_dir/market.log" "groupId=market] Adding newly assigned partitions:" "$MARKET_PID" 120
  case $? in
    0) ;;
    2) LIVE_FAIL_REASON="market 이 시작 중에 끝남"; return 1 ;;
    *) LIVE_FAIL_REASON="market 이 120초 안에 준비되지 않음"; return 1 ;;
  esac

  echo "collector 기동(실제 시세, 종목 1개)"
  (
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:$port"
    export COLLECTOR_MARKET_DATA_SOURCE=broker BROKER_BASE_URL="$base_url" BROKER_APP_KEY="$app_key" BROKER_APP_SECRET="$app_secret"
    export COLLECTOR_SYMBOLS="$symbol"
    export COLLECTOR_SCHEDULE_ENABLED=false COLLECTOR_PRE_OPEN_ENABLED=false COLLECTOR_INSIGHT_SCHEDULE_ENABLED=false
    # 콘솔 프로듀서 기동에 몇 초 걸려 기본 3초 기준이면 tick 이 오래된 것으로 버려질 수 있다.
    export COLLECTOR_MAX_TICK_AGE_MS=30000
    cd "$svc_dir/collector" && exec "$JAVA_HOME_21/bin/java" -jar "$COLLECTOR_JAR"
  ) > "$svc_dir/collector.log" 2>&1 &
  COLLECTOR_PID=$!
  # collector 그룹은 latest 부터 읽으므로 파티션을 받기 전에 보낸 tick 은 놓친다.
  wait_for_log "$svc_dir/collector.log" "groupId=collector] Adding newly assigned partitions: schedule.tick" "$COLLECTOR_PID" 120
  case $? in
    0) ;;
    2) LIVE_FAIL_REASON="collector 가 시작 중에 끝남"; return 1 ;;
    *) LIVE_FAIL_REASON="collector 가 120초 안에 준비되지 않음"; return 1 ;;
  esac
  sleep 3

  echo "tick 1건 발행"
  KAFKA_CONTAINER="$KAFKA_NAME" KAFKA_BOOTSTRAP="localhost:$port" bash scripts/publish-tick.sh collect >/dev/null \
    || { LIVE_FAIL_REASON="tick 을 보내지 못함"; return 1; }

  echo "테스트 전용 데이터베이스에서 다시 읽어 확인(개수만 출력)"
  (
    export QUOTE_FLOW_LIVE_DB_URL="$PG_URL" QUOTE_FLOW_LIVE_DB_USER="$M_USER" QUOTE_FLOW_LIVE_DB_PASSWORD="$M_PW"
    export QUOTE_FLOW_LIVE_EXPECTED=1 QUOTE_FLOW_LIVE_WAIT_SECONDS=60 QUOTE_FLOW_LIVE_COUNTS_FILE="$TMP/results/live/counts.json"
    "$PY" -m pytest -c scripts/pytest.ini scripts/tests/test_quote_flow_live.py --junitxml="$TMP/results/live/py/scripts.xml"
  ) || { LIVE_FAIL_REASON="저장 확인 실패"; return 1; }
}

log "6. 시세 흐름 실제"
t=$SECONDS
if [ "$LIVE" = 0 ]; then
  echo "건너뜀($LIVE_SKIP_REASON)"
  stage live "시세 흐름 실제" skip "$LIVE_SKIP_REASON" 0
elif [ "$DB_READY" = 0 ]; then
  stage live "시세 흐름 실제" fail "DB 준비 실패로 돌리지 못함" 0
elif run_live; then
  stage live "시세 흐름 실제" ok "" $((SECONDS - t)) live
else
  echo "시세 흐름 실제 실패: $LIVE_FAIL_REASON"
  stage live "시세 흐름 실제" fail "$LIVE_FAIL_REASON" $((SECONDS - t)) live
fi

# ---------------------------------------------------------------- 7. 정리

log "7. 정리"
t=$SECONDS
if cleanup_resources; then
  stage cleanup "정리" ok "이 실행이 만든 것만 지움, $PG_CONTAINER 는 켜 둠" $((SECONDS - t))
else
  stage cleanup "정리" fail "남은 것이 있음(위 안내 참고)" $((SECONDS - t))
fi
CLEANED=1
M_PW="" N_PW="" B_PW=""

# ---------------------------------------------------------------- 8. 결과 표

log "8. 결과 (docs/test-map 기준)"
if [ "$LIVE" = 1 ]; then
  python3 scripts/run_report.py report --results "$TMP/results" --live
else
  python3 scripts/run_report.py report --results "$TMP/results"
fi
rc=$?
total=$((SECONDS - STARTED_AT))
echo "전체 시간: $((total / 60))분 $((total % 60))초"
exit "$rc"
