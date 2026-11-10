#!/usr/bin/env bash
# 전체 실행 스크립트. 단위·가상 테스트와 실제 DB 테스트를 항상 돌리고, --live 일 때만 시세 흐름과 브리핑·메일 실제 테스트를
# 돌린 뒤 테스트 지도(docs/test-map)의 기능별 '돌았음 / 건너뜀(이유) / 실패' 표를 출력한다. 하나라도 실패하면 1로 끝난다.
#
# 사용법: scripts/test-all.sh [--live] [--plan]
#   --live  실제 외부를 부른다(시세 토큰 1 + 시세 1 + LLM 1 + 메일 1통). 사람이 직접 결정해서만 쓴다.
#   --plan  아무것도 띄우지 않고 실행 순서만 보여 준다.
#
# 실제 DB 테스트는 개발용 stock-postgres 안에 이 실행만의 stock_test_run_<시각>_<pid> 데이터베이스를 만들어 쓰고 지운다.
# 꺼져 있던 stock-postgres 는 켜고 끝나도 끄지 않는다. 서비스 계정은 만들거나 바꾸지 않는다.
# 서비스 계정 비밀번호는 .env 에서, --live 의 키는 Parameter Store 에서 필요한 키만 골라 읽어 자식 프로세스 환경변수로만 넘기고 출력하지 않는다.
#
# 환경변수(모두 선택):
#   TEST_ALL_ENV_FILE  .env 위치(기본 저장소 루트의 .env)
#   TEST_ALL_PYTHON    브리핑 venv 파이썬(기본 services/briefing/.venv/bin/python)
#   JAVA_HOME_21       JDK 21 위치(없으면 /usr/libexec/java_home -v 21 로 찾음)
#   TEST_ALL_AWS_PROFILE  --live 가 Parameter Store 를 읽을 AWS CLI 프로필. --live 에서는 필수이고,
#                         엉뚱한 계정을 읽지 않도록 기본 자격증명 방식으로 넘어가지 않는다
#   --live 전용: BROKER_BASE_URL, BROKER_APP_KEY, BROKER_APP_SECRET, TEST_ALL_LIVE_SYMBOL(6자리),
#               ANTHROPIC_API_KEY, SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, NOTIFIER_SENDER, NOTIFIER_RECIPIENT
#               (선택: SMTP_PORT, BRIEFING_LLM_MODEL)
#               (BRIEFING_LLM_MODEL 을 뺀 위 키는) 실행할 때마다 Parameter Store 에서 새로 읽어 임시 폴더에만 두고,
#               실행 환경변수와 .env 의 같은 키는 쓰지 않는다. 하나라도 비면 시세를 부르기 전에 실패로 끝낸다.
#               메일 발송 스위치는 이 스크립트가 띄운 테스트용 notifier 프로세스에만 켜고(.env 는 바꾸지 않음),
#               제목 앞에 [테스트]를 붙이고 시간당 1통으로 묶는다. LLM 리포트 본문은 받은 메일과
#               build/test-all-live/briefing-<실행 이름>.txt(권한 600, git 제외)에만 남는다.
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
  echo "4. 실제 DB 준비: stock-postgres 확인(꺼져 있으면 켬), stock_test_run_<시각>_<pid> 데이터베이스와 market·news·briefing 스키마 생성,"
  echo "   브리핑 테이블은 Flyway(flyway/flyway 이미지, services/briefing/db/migration)로 만듦. 브리핑보다 먼저 적용되며, 이미지가 없으면 처음 한 번 받음"
  echo "5. 실제 DB 테스트: 지도에서 고른 market·news·브리핑 실제 DB 테스트"
  if [ "$LIVE" = 1 ]; then
    echo "6. 시세 흐름 실제: 일회용 Kafka, market(테스트 전용 데이터베이스), collector(실제 시세, 종목 1개), tick 1건, 저장 확인"
    echo "6b. 브리핑·메일 실제: news·notifier(발송 켬, [테스트], 1통)·briefing(실제 LLM) 기동, 가짜 변동·기사·수집 완료 넣기,"
    echo "    리포트(generatedBy=llm)와 메일 발송 1통 확인, 본문은 build/test-all-live/ 에 저장"
  else
    echo "6. 시세 흐름 실제: 건너뜀($LIVE_SKIP_REASON)"
    echo "6b. 브리핑·메일 실제: 건너뜀($LIVE_SKIP_REASON)"
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
# 브리핑 테이블을 만드는 Flyway. 버전은 market·news가 쓰는 Flyway와 맞춘다. infra/compose/docker-compose.yml의 briefing-migrate와 같다.
FLYWAY_IMAGE="flyway/flyway:12.4.0"
ENV_FILE="${TEST_ALL_ENV_FILE:-$ROOT/.env}"
PY="${TEST_ALL_PYTHON:-$ROOT/services/briefing/.venv/bin/python}"
LOCK_DIR="$ROOT/build/test-all.lock"
LIVE_OUT_DIR="$ROOT/build/test-all-live"
JAVA_SERVICES="collector market news notifier"

# 바깥에서 들어온 켜는 변수가 기본 단계에 섞이면 엉뚱한 데이터베이스로 실제 DB 테스트가 돈다. 필요한 단계에서만 넣는다.
unset MARKET_IT_DB_URL MARKET_IT_DB_USER MARKET_IT_DB_PASSWORD NEWS_IT_DB_URL NEWS_IT_DB_USER NEWS_IT_DB_PASSWORD \
  BRIEFING_IT_DB_URL BRIEFING_IT_DB_USER BRIEFING_IT_DB_PASSWORD \
  QUOTE_FLOW_LIVE_DB_URL QUOTE_FLOW_LIVE_DB_USER QUOTE_FLOW_LIVE_DB_PASSWORD \
  BRIEFING_FLOW_LIVE_KAFKA BRIEFING_FLOW_LIVE_RUN_ID BRIEFING_FLOW_LIVE_DB_URL BRIEFING_FLOW_LIVE_DB_USER \
  BRIEFING_FLOW_LIVE_DB_PASSWORD BRIEFING_FLOW_LIVE_BRIEFING_LOG BRIEFING_FLOW_LIVE_NOTIFIER_LOG \
  BRIEFING_FLOW_LIVE_REPORT_FILE QUOTE_FLOW_LIVE_SINCE BRIEFING_FLOW_LIVE_SINCE

TMP=""
LOCKED=0
DB_CREATED=0
KAFKA_STARTED=0
MARKET_PID=""
COLLECTOR_PID=""
NEWS_PID=""
NOTIFIER_PID=""
BRIEFING_PID=""
MARKET_JAR=""
COLLECTOR_JAR=""
NEWS_JAR=""
NOTIFIER_JAR=""
KAFKA_PORT=""
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
  # 두 번째 인자는 이 실행이 띄운 프로세스의 명령줄에 있어야 하는 문자열(jar 경로, 브리핑은 임시 폴더)이다.
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
  stop_service "$NOTIFIER_PID" "$NOTIFIER_JAR" notifier || bad=1
  NOTIFIER_PID=""
  stop_service "$BRIEFING_PID" "$TMP/svc/briefing" briefing || bad=1
  BRIEFING_PID=""
  stop_service "$NEWS_PID" "$NEWS_JAR" news || bad=1
  NEWS_PID=""
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

migrate_briefing() {
  # 브리핑은 테이블을 만들지 않으므로 브리핑 테스트와 기동보다 먼저 Flyway 로 만든다.
  # 서비스 계정으로 붙고, 비밀번호는 명령줄이 아니라 환경변수로만 넘긴다. 컨테이너는 stock-postgres 의 네트워크를 같이 쓴다.
  local out
  if ! out=$(FLYWAY_PASSWORD="$B_PW" docker run --rm --network "container:$PG_CONTAINER" \
      -v "$ROOT/services/briefing/db/migration:/flyway/sql:ro" \
      -e FLYWAY_PASSWORD -e FLYWAY_USER="$B_USER" -e FLYWAY_URL="jdbc:postgresql://localhost:5432/$DB_NAME" \
      -e FLYWAY_SCHEMAS=briefing -e FLYWAY_DEFAULT_SCHEMA=briefing -e FLYWAY_CREATE_SCHEMAS=false \
      -e FLYWAY_LOCATIONS=filesystem:/flyway/sql "$FLYWAY_IMAGE" migrate 2>&1); then
    printf '%s\n' "$out" | tail -15
    return 1
  fi
  printf '%s\n' "$out" | grep -E 'Successfully applied|up to date' | head -2
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
  if ! migrate_briefing; then DB_FAIL_REASON="브리핑 테이블 Flyway 적용 실패(docker 이미지 $FLYWAY_IMAGE)"; return 1; fi
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
LIVE_SINCE=""
QUOTE_LIVE_OK=0

wait_for_log() {
  # 서비스 로그에 문구가 나올 때까지 기다린다. 로그 내용은 출력하지 않는다(종목 코드가 있을 수 있음).
  local file=$1 pattern=$2 pid=$3 limit=$4 waited=0
  until grep -qF -- "$pattern" "$file" 2>/dev/null; do
    kill -0 "$pid" 2>/dev/null || return 2
    [ "$waited" -ge "$limit" ] && return 1
    sleep 1; waited=$((waited + 1))
  done
}

# --live 의 키는 .env 복사본이 낡아도 영향받지 않게 실행할 때마다 Parameter Store 에서 읽는다.
# 값은 임시 폴더의 파일(권한 600)에만 두고 끝나면 임시 폴더와 함께 지운다. 로더는 키 이름만 출력한다.
LIVE_PARAMS=""

fetch_live_params() {
  LIVE_PARAMS="$TMP/live-params.env"
  "$PY" "$ROOT/scripts/load_config.py" --env-file "$LIVE_PARAMS" --profile "$TEST_ALL_AWS_PROFILE" > "$TMP/load_config.log" 2>&1
}

# 임시 폴더는 끝나면 지워지므로, 읽기 실패 이유(키 이름만 있음)는 결과 폴더에 옮겨 둔다.
keep_loader_log() {
  local kept="$LIVE_OUT_DIR/load_config-${RUN_ID}.log"
  mkdir -p "$LIVE_OUT_DIR" && cp "$TMP/load_config.log" "$kept" 2>/dev/null && chmod 600 "$kept" && printf '%s' "${kept#"$ROOT"/}"
}

live_value() {
  [ -n "$LIVE_PARAMS" ] && [ -f "$LIVE_PARAMS" ] || return 0
  grep -E "^$1=" "$LIVE_PARAMS" | head -1 | cut -d= -f2- | tr -d "\"'\r"
}

# 브리핑·메일 단계까지 필요한 키. 시세를 부르기 전에 모두 확인해 메일 설정이 빠졌는데 시세만 부르는 일이 없게 한다.
LIVE_KEYS="BROKER_BASE_URL BROKER_APP_KEY BROKER_APP_SECRET TEST_ALL_LIVE_SYMBOL ANTHROPIC_API_KEY SMTP_HOST SMTP_USERNAME SMTP_PASSWORD NOTIFIER_SENDER NOTIFIER_RECIPIENT"

missing_live_keys() {
  local key missing=""
  for key in $LIVE_KEYS; do
    [ -n "$(live_value "$key")" ] || missing="$missing${missing:+·}$key"
  done
  printf '%s' "$missing"
}

service_jar() {
  local jar
  jar=$(ls "services/$1/build/libs/"*.jar 2>/dev/null | grep -v -- '-plain' | head -1)
  [ -n "$jar" ] && printf '%s' "$ROOT/$jar"
}

run_live() {
  local base_url app_key app_secret symbol port waited svc_dir missing
  if [ "$PY_OK" = 0 ]; then LIVE_FAIL_REASON="브리핑 venv 가 없어 Parameter Store 를 읽지 못함"; return 1; fi
  if [ -z "${TEST_ALL_AWS_PROFILE:-}" ]; then LIVE_FAIL_REASON="TEST_ALL_AWS_PROFILE 이 없음(기본 자격증명은 쓰지 않음)"; return 1; fi
  if ! fetch_live_params; then LIVE_FAIL_REASON="Parameter Store 읽기 실패($(keep_loader_log) 참고, 키 이름만 있음)"; return 1; fi
  missing=$(missing_live_keys)
  if [ -n "$missing" ]; then LIVE_FAIL_REASON="키 없음($missing)"; return 1; fi
  base_url=$(live_value BROKER_BASE_URL)
  # 다른 파라미터 값(예: 메일 주소)이 잘못 들어가면 요청 주소를 만들다 조용히 실패하므로 시세를 부르기 전에 막는다.
  if ! [[ "$base_url" =~ ^https://[^/[:space:]]+$ ]]; then LIVE_FAIL_REASON="BROKER_BASE_URL 이 https://<호스트>[:포트] 형식이 아님"; return 1; fi
  app_key=$(live_value BROKER_APP_KEY)
  app_secret=$(live_value BROKER_APP_SECRET)
  symbol=$(live_value TEST_ALL_LIVE_SYMBOL)
  if ! [[ "$symbol" =~ ^[0-9]{6}$ ]]; then LIVE_FAIL_REASON="TEST_ALL_LIVE_SYMBOL 이 6자리 숫자가 아님"; return 1; fi
  if [ "$JAVA_OK" = 0 ] || [ "$PY_OK" = 0 ]; then LIVE_FAIL_REASON="JDK 21 또는 브리핑 venv 가 없음"; return 1; fi

  echo "bootJar 빌드"
  ./gradlew -q :services:collector:bootJar :services:market:bootJar :services:news:bootJar :services:notifier:bootJar \
    || { LIVE_FAIL_REASON="bootJar 빌드 실패"; return 1; }
  MARKET_JAR=$(service_jar market)
  COLLECTOR_JAR=$(service_jar collector)
  NEWS_JAR=$(service_jar news)
  NOTIFIER_JAR=$(service_jar notifier)
  if [ -z "$MARKET_JAR" ] || [ -z "$COLLECTOR_JAR" ] || [ -z "$NEWS_JAR" ] || [ -z "$NOTIFIER_JAR" ]; then
    LIVE_FAIL_REASON="bootJar 를 찾지 못함"; return 1
  fi

  port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  KAFKA_PORT=$port
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
  # MARKET_PREV_CLOSE_FIELDS 를 빈 값으로 넘겨 전일 종가 기준 감지를 끈다. 스냅샷이 1개라 구간 감지도 일어나지 않으므로
  # market 이 스스로 변동을 내지 않고, 브리핑·메일 단계의 변동은 스크립트가 넣은 1건뿐이다.
  (
    export MARKET_PREV_CLOSE_FIELDS=
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
  wait_for_log "$svc_dir/collector.log" "groupId=collector] Adding newly assigned partitions: [schedule.tick" "$COLLECTOR_PID" 120
  case $? in
    0) ;;
    2) LIVE_FAIL_REASON="collector 가 시작 중에 끝남"; return 1 ;;
    *) LIVE_FAIL_REASON="collector 가 120초 안에 준비되지 않음"; return 1 ;;
  esac
  sleep 3

  # 같은 테스트 DB 에 실제 DB 테스트가 넣은 가짜 행이 남아 있어, 이 시각 뒤에 저장된 행만 이번 실행의 결과로 센다.
  LIVE_SINCE=$(echo "SELECT now();" | admin_sql) || { LIVE_FAIL_REASON="DB 시각을 읽지 못함"; return 1; }
  echo "tick 1건 발행"
  KAFKA_CONTAINER="$KAFKA_NAME" KAFKA_BOOTSTRAP="localhost:$port" bash scripts/publish-tick.sh collect >/dev/null \
    || { LIVE_FAIL_REASON="tick 을 보내지 못함"; return 1; }

  echo "테스트 전용 데이터베이스에서 다시 읽어 확인(개수만 출력)"
  (
    export QUOTE_FLOW_LIVE_DB_URL="$PG_URL" QUOTE_FLOW_LIVE_DB_USER="$M_USER" QUOTE_FLOW_LIVE_DB_PASSWORD="$M_PW"
    export QUOTE_FLOW_LIVE_SINCE="$LIVE_SINCE" QUOTE_FLOW_LIVE_EXPECTED=1 QUOTE_FLOW_LIVE_WAIT_SECONDS=60 QUOTE_FLOW_LIVE_COUNTS_FILE="$TMP/results/live/counts.json"
    "$PY" -m pytest -c scripts/pytest.ini scripts/tests/test_quote_flow_live.py --junitxml="$TMP/results/live/py/scripts.xml"
  ) || { LIVE_FAIL_REASON="저장 확인 실패"; return 1; }
}

# ---------------------------------------------------------------- 6b. 브리핑·메일 실제 (--live)

LIVE_BRIEF_FAIL_REASON=""
LIVE_REPORT_FILE="$LIVE_OUT_DIR/briefing-${RUN_ID}.txt"

run_live_briefing() {
  local svc_dir="$TMP/svc" bootstrap="localhost:$KAFKA_PORT" llm_key llm_model
  llm_key=$(live_value ANTHROPIC_API_KEY)
  llm_model=$(env_or_dotenv BRIEFING_LLM_MODEL)
  mkdir -p "$svc_dir/news" "$svc_dir/notifier" "$svc_dir/briefing"

  echo "news 기동(테스트 전용 데이터베이스 news 스키마)"
  (
    export SPRING_DATASOURCE_URL="$JDBC_URL?currentSchema=news" SPRING_DATASOURCE_USERNAME="$N_USER" SPRING_DATASOURCE_PASSWORD="$N_PW"
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="$bootstrap"
    cd "$svc_dir/news" && exec "$JAVA_HOME_21/bin/java" -jar "$NEWS_JAR"
  ) > "$svc_dir/news.log" 2>&1 &
  NEWS_PID=$!
  # Flyway 로 기사 테이블을 만든 뒤에야 리스너가 파티션을 받는다.
  wait_for_log "$svc_dir/news.log" "groupId=news] Adding newly assigned partitions:" "$NEWS_PID" 120
  case $? in
    0) ;;
    2) LIVE_BRIEF_FAIL_REASON="news 가 시작 중에 끝남"; return 1 ;;
    *) LIVE_BRIEF_FAIL_REASON="news 가 120초 안에 준비되지 않음"; return 1 ;;
  esac

  echo "notifier 기동(테스트용: 실제 발송, 제목 앞 [테스트], 시간당 1통)"
  (
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="$bootstrap"
    # 발송 스위치는 이 프로세스 환경에만 둔다. .env 는 읽지도 쓰지도 않는다(임시 폴더에서 띄움).
    export NOTIFIER_SEND_ENABLED=true NOTIFIER_SUBJECT_PREFIX="[테스트]" NOTIFIER_MAX_PER_HOUR=1
    SMTP_HOST=$(live_value SMTP_HOST) SMTP_USERNAME=$(live_value SMTP_USERNAME)
    SMTP_PASSWORD=$(live_value SMTP_PASSWORD) SMTP_PORT=$(live_value SMTP_PORT)
    NOTIFIER_SENDER=$(live_value NOTIFIER_SENDER) NOTIFIER_RECIPIENT=$(live_value NOTIFIER_RECIPIENT)
    export SMTP_HOST SMTP_USERNAME SMTP_PASSWORD NOTIFIER_SENDER NOTIFIER_RECIPIENT
    if [ -n "$SMTP_PORT" ]; then export SMTP_PORT; else unset SMTP_PORT; fi
    # 제목의 한글 머리말이 환경변수에서 깨지지 않게 JVM 이 UTF-8 로 읽게 한다.
    export LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
    cd "$svc_dir/notifier" && exec "$JAVA_HOME_21/bin/java" -jar "$NOTIFIER_JAR"
  ) > "$svc_dir/notifier.log" 2>&1 &
  NOTIFIER_PID=$!
  # notifier 는 처음 시작할 때 latest 부터 읽으므로 브리핑이 나오기 전에 파티션을 받아 두어야 메일을 놓치지 않는다.
  wait_for_log "$svc_dir/notifier.log" "groupId=notifier] Adding newly assigned partitions: [briefing.generated" "$NOTIFIER_PID" 120
  case $? in
    0) ;;
    2) LIVE_BRIEF_FAIL_REASON="notifier 가 시작 중에 끝남"; return 1 ;;
    *) LIVE_BRIEF_FAIL_REASON="notifier 가 120초 안에 준비되지 않음"; return 1 ;;
  esac

  echo "briefing 기동(테스트 전용 데이터베이스, 실제 LLM)"
  (
    export KAFKA_BOOTSTRAP="$bootstrap" POSTGRES_DB="$DB_NAME" BRIEFING_DB_USER="$B_USER" BRIEFING_DB_PASSWORD="$B_PW"
    export ANTHROPIC_API_KEY="$llm_key" PYTHONPATH="$ROOT/services/briefing/src" PYTHONUNBUFFERED=1
    if [ -n "$llm_model" ]; then export BRIEFING_LLM_MODEL="$llm_model"; else unset BRIEFING_LLM_MODEL; fi
    # 캐시 위치를 이 실행의 임시 폴더로 두면 명령줄에 임시 폴더가 남아 정리할 때 이 실행의 프로세스인지 알 수 있다.
    cd "$svc_dir/briefing" && exec "$PY" -X "pycache_prefix=$svc_dir/briefing/pycache" -m briefing.main
  ) > "$svc_dir/briefing.log" 2>&1 &
  BRIEFING_PID=$!
  wait_for_log "$svc_dir/briefing.log" "briefing started:" "$BRIEFING_PID" 60
  case $? in
    0) ;;
    2) LIVE_BRIEF_FAIL_REASON="briefing 이 시작 중에 끝남"; return 1 ;;
    *) LIVE_BRIEF_FAIL_REASON="briefing 이 60초 안에 준비되지 않음"; return 1 ;;
  esac
  if ! grep -qF "briefing reports will be written by the LLM" "$svc_dir/briefing.log"; then
    LIVE_BRIEF_FAIL_REASON="briefing 이 규칙 기반으로 시작됨(ANTHROPIC_API_KEY 확인)"; return 1
  fi

  echo "가짜 변동·근거 기사·수집 완료 넣기"
  (
    export LIVE_BRIEFING_DB_URL="$PG_URL" LIVE_BRIEFING_KAFKA="$bootstrap" LIVE_BRIEFING_RUN_ID="$RUN_ID" LIVE_BRIEFING_SINCE="$LIVE_SINCE"
    export LIVE_BRIEFING_MARKET_USER="$M_USER" LIVE_BRIEFING_MARKET_PASSWORD="$M_PW"
    export LIVE_BRIEFING_NEWS_USER="$N_USER" LIVE_BRIEFING_NEWS_PASSWORD="$N_PW"
    "$PY" scripts/live_briefing.py seed
  ) || { LIVE_BRIEF_FAIL_REASON="변동·기사 넣기 실패"; return 1; }

  echo "리포트·메일 확인(개수와 생성 방식만 출력, 본문은 $LIVE_REPORT_FILE)"
  (
    export BRIEFING_FLOW_LIVE_KAFKA="$bootstrap" BRIEFING_FLOW_LIVE_RUN_ID="$RUN_ID" BRIEFING_FLOW_LIVE_SINCE="$LIVE_SINCE"
    export BRIEFING_FLOW_LIVE_DB_URL="$PG_URL" BRIEFING_FLOW_LIVE_DB_USER="$B_USER" BRIEFING_FLOW_LIVE_DB_PASSWORD="$B_PW"
    export BRIEFING_FLOW_LIVE_BRIEFING_LOG="$svc_dir/briefing.log" BRIEFING_FLOW_LIVE_NOTIFIER_LOG="$svc_dir/notifier.log"
    export BRIEFING_FLOW_LIVE_REPORT_FILE="$LIVE_REPORT_FILE" BRIEFING_FLOW_LIVE_COUNTS_FILE="$TMP/results/live/briefing-counts.json"
    "$PY" -m pytest -c scripts/pytest.ini scripts/tests/test_briefing_flow_live.py --junitxml="$TMP/results/live/py/scripts-briefing.xml"
  ) || { LIVE_BRIEF_FAIL_REASON="리포트·메일 확인 실패"; return 1; }
}

log "6. 시세 흐름 실제"
t=$SECONDS
if [ "$LIVE" = 0 ]; then
  echo "건너뜀($LIVE_SKIP_REASON)"
  stage live "시세 흐름 실제" skip "$LIVE_SKIP_REASON" 0
elif [ "$DB_READY" = 0 ]; then
  stage live "시세 흐름 실제" fail "DB 준비 실패로 돌리지 못함" 0
elif run_live; then
  stage live "시세 흐름 실제" ok "" $((SECONDS - t)) live/py/scripts.xml
  QUOTE_LIVE_OK=1
else
  echo "시세 흐름 실제 실패: $LIVE_FAIL_REASON"
  stage live "시세 흐름 실제" fail "$LIVE_FAIL_REASON" $((SECONDS - t)) live/py/scripts.xml
fi

log "6b. 브리핑·메일 실제"
t=$SECONDS
if [ "$LIVE" = 0 ]; then
  echo "건너뜀($LIVE_SKIP_REASON)"
  stage livebrief "브리핑·메일 실제" skip "$LIVE_SKIP_REASON" 0
elif [ "${QUOTE_LIVE_OK:-0}" = 0 ]; then
  # 브리핑 서비스를 띄우지 않았으므로 LLM·메일은 부르지 않았다.
  stage livebrief "브리핑·메일 실제" fail "시세 흐름 실제가 통과하지 않아 돌리지 않음" 0
elif run_live_briefing; then
  stage livebrief "브리핑·메일 실제" ok "리포트 파일 ${LIVE_REPORT_FILE#"$ROOT"/}" $((SECONDS - t)) live/py/scripts-briefing.xml
else
  echo "브리핑·메일 실제 실패: $LIVE_BRIEF_FAIL_REASON"
  detail="$LIVE_BRIEF_FAIL_REASON"
  [ -f "$LIVE_REPORT_FILE" ] && detail="$detail, 리포트 파일 ${LIVE_REPORT_FILE#"$ROOT"/}"
  stage livebrief "브리핑·메일 실제" fail "$detail" $((SECONDS - t)) live/py/scripts-briefing.xml
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
