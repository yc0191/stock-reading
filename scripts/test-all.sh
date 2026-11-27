#!/usr/bin/env bash
# 전체 실행 스크립트. 단위·가상 테스트와 실제 DB 테스트, 구성 종목 리허설(가짜 운용사 서버)을 항상 돌리고, --live 일 때만 시세 흐름과 브리핑·메일 실제 테스트를
# 돌린 뒤 테스트 지도(docs/test-map)의 기능별 '돌았음 / 건너뜀(이유) / 실패' 표를 출력한다. 하나라도 실패하면 1로 끝난다.
#
# 사용법: scripts/test-all.sh [--live] [--plan]
#   --live  실제 외부를 부른다(시세 토큰 1 + 시세 1 + LLM 1 + 메일 1통, LLM이 근거 약함을 표시하면 재검색의 뉴스 검색
#           최대 30회와 LLM 몇 회가 더 든다). 사람이 직접 결정해서만 쓴다.
#   --plan  아무것도 띄우지 않고 실행 순서만 보여 준다.
#
# 실제 DB 테스트는 개발용 stock-postgres 안에 이 실행만의 stock_test_run_<시각>_<pid> 데이터베이스를 만들어 쓰고 지운다.
# 꺼져 있던 stock-postgres 는 켜고 끝나도 끄지 않는다. 서비스 계정은 만들거나 바꾸지 않는다(personal 계정 4개도 .env 에 있고 이미 만들어져 있어야 한다).
# 서비스 계정 비밀번호는 .env 에서, --live 의 키는 Parameter Store 에서 필요한 키만 골라 읽어 자식 프로세스 환경변수로만 넘기고 출력하지 않는다.
#
# 환경변수(모두 선택):
#   TEST_ALL_ENV_FILE  .env 위치(기본 저장소 루트의 .env)
#   TEST_ALL_PYTHON    브리핑 venv 파이썬(기본 services/briefing/.venv/bin/python)
#   TEST_ALL_OPS_PYTHON  확인기 venv 파이썬(기본 services/ops-checker/.venv/bin/python)
#   JAVA_HOME_21       JDK 21 위치(없으면 /usr/libexec/java_home -v 21 로 찾음)
#   TEST_ALL_AWS_PROFILE  --live 가 Parameter Store 를 읽을 AWS CLI 프로필. --live 에서는 필수이고,
#                         엉뚱한 계정을 읽지 않도록 기본 자격증명 방식으로 넘어가지 않는다
#   --live 전용: BROKER_BASE_URL, BROKER_APP_KEY, BROKER_APP_SECRET, TEST_ALL_LIVE_SYMBOL(6자리),
#               ANTHROPIC_API_KEY, SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, TEST_ALL_LIVE_RECIPIENT,
#               NEWS_SEARCH_CLIENT_ID, NEWS_SEARCH_CLIENT_SECRET
#               (선택: SMTP_PORT, BRIEFING_LLM_MODEL)
#               (비밀이 아닌 뉴스 검색 설정 NEWS_SEARCH_BASE_URL, NEWS_SEARCH_ID_HEADER, NEWS_SEARCH_SECRET_HEADER 는 필수이며
#                실행 환경변수나 .env 에서 읽는다. 하나라도 비면 시세를 부르기 전에 실패로 끝낸다)
#               (BRIEFING_LLM_MODEL 을 뺀 위 키는) 실행할 때마다 Parameter Store 에서 새로 읽어 임시 폴더에만 두고,
#               실행 환경변수와 .env 의 같은 키는 쓰지 않는다. 하나라도 비면 시세를 부르기 전에 실패로 끝낸다.
#               메일 발송 스위치는 이 스크립트가 띄운 테스트용 notifier 프로세스에만 켜고(.env 는 바꾸지 않음),
#               제목 앞에 [테스트]를 붙인다. LLM 리포트 본문은 받은 메일과
#               build/test-all-live/briefing-<실행 이름>.txt(권한 600, git 제외)에만 남는다.
set -uo pipefail

usage() {
  sed -n '5,8p' "$0" | sed 's/^# \{0,1\}//'
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
  echo "2b. 확인기: pytest services/ops-checker (확인기 venv)"
  echo "3. 설정 로더·지도 검사: pytest -c scripts/pytest.ini scripts/tests"
  echo "4. 실제 DB 준비: stock-postgres 확인(꺼져 있으면 켬), stock_test_run_<시각>_<pid> 데이터베이스와 market·news·briefing 스키마 생성,"
  echo "   테이블은 Flyway(flyway/flyway 이미지)로 만듦: market → personal 계정·권한(infra/db/personal/init.sql) → personal 순서(personal 이 market 표를 가리킴), 브리핑은 그다음."
  echo "   이미지가 없으면 처음 한 번 받음"
  echo "5. 실제 DB 테스트: 지도에서 고른 market·news·notifier·브리핑·확인기 실제 DB 테스트"
  echo "5b. 구성 종목 리허설: 일회용 Kafka, 가짜 운용사 서버, market·collector(테스트 전용 데이터베이스), 수동 실행 명령 collect-etf-members 1건,"
  echo "    원문·이력·구성 연결 표 확인 뒤 이 단계가 띄운 것만 정리(외부 호출 없음)"
  if [ "$LIVE" = 1 ]; then
    echo "6. 시세 흐름 실제: 일회용 Kafka, market(테스트 전용 데이터베이스), collector(실제 시세, 종목 1개), 수동 실행 명령 collect 1건, 저장 확인"
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
# market → personal 계정·권한 → personal 표를 만드는 함수(migrate_market, init_personal, migrate_personal). 매뉴얼 실행 스크립트와 같이 쓴다.
# shellcheck source=scripts/personal-db-lib.sh
. "$ROOT/scripts/personal-db-lib.sh"
ENV_FILE="${TEST_ALL_ENV_FILE:-$ROOT/.env}"
PY="${TEST_ALL_PYTHON:-$ROOT/services/briefing/.venv/bin/python}"
OPS_PY="${TEST_ALL_OPS_PYTHON:-$ROOT/services/ops-checker/.venv/bin/python}"
LOCK_DIR="$ROOT/build/test-all.lock"
LIVE_OUT_DIR="$ROOT/build/test-all-live"
JAVA_SERVICES="collector market news notifier"

# 바깥에서 들어온 켜는 변수가 기본 단계에 섞이면 엉뚱한 데이터베이스로 실제 DB 테스트가 돈다. 필요한 단계에서만 넣는다.
unset MARKET_IT_DB_URL MARKET_IT_DB_USER MARKET_IT_DB_PASSWORD MARKET_IT_DB_MIGRATION_SCHEMA NEWS_IT_DB_URL NEWS_IT_DB_USER NEWS_IT_DB_PASSWORD NEWS_IT_DB_MIGRATION_SCHEMA \
  WRITER_IT_DB_USER WRITER_IT_DB_PASSWORD COLLECTOR_IT_DB_URL COLLECTOR_IT_DB_USER COLLECTOR_IT_DB_PASSWORD NOTIFIER_IT_DB_URL NOTIFIER_IT_DB_USER NOTIFIER_IT_DB_PASSWORD \
  BRIEFING_IT_DB_URL BRIEFING_IT_DB_USER BRIEFING_IT_DB_PASSWORD OPS_IT_DB_URL OPS_IT_DB_USER OPS_IT_DB_PASSWORD \
  QUOTE_FLOW_LIVE_DB_URL QUOTE_FLOW_LIVE_DB_USER QUOTE_FLOW_LIVE_DB_PASSWORD \
  BRIEFING_FLOW_LIVE_KAFKA BRIEFING_FLOW_LIVE_RUN_ID BRIEFING_FLOW_LIVE_DB_URL BRIEFING_FLOW_LIVE_DB_USER \
  BRIEFING_FLOW_LIVE_DB_PASSWORD BRIEFING_FLOW_LIVE_BRIEFING_LOG BRIEFING_FLOW_LIVE_NOTIFIER_LOG \
  BRIEFING_FLOW_LIVE_REPORT_FILE QUOTE_FLOW_LIVE_SINCE BRIEFING_FLOW_LIVE_SINCE \
  MEMBERS_FLOW_DB_URL MEMBERS_FLOW_DB_USER MEMBERS_FLOW_DB_PASSWORD MEMBERS_FLOW_REQUEST_LOG MEMBERS_FLOW_WAIT_SECONDS

TMP=""
LOCKED=0
DB_CREATED=0
KAFKA_STARTED=0
MARKET_PID=""
COLLECTOR_PID=""
NEWS_PID=""
NOTIFIER_PID=""
BRIEFING_PID=""
FAKE_FUND_PID=""
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

# 단계별 함수와 그 함수가 쓰는 변수는 scripts/test-all-*.sh 에 나눠 두고 여기서 읽는다.
# 정리(cleanup), 실제 DB 준비·테스트(db), 실제 호출 --live(live), 구성 종목 리허설(rehearsal).
# shellcheck source=scripts/test-all-cleanup.sh
. "$ROOT/scripts/test-all-cleanup.sh"
# shellcheck source=scripts/test-all-db.sh
. "$ROOT/scripts/test-all-db.sh"
# shellcheck source=scripts/test-all-live.sh
. "$ROOT/scripts/test-all-live.sh"
# shellcheck source=scripts/test-all-rehearsal.sh
. "$ROOT/scripts/test-all-rehearsal.sh"

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
OPS_PY_OK=1
[ -x "$OPS_PY" ] || OPS_PY_OK=0

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

log "2b. 확인기"
t=$SECONDS
if [ "$OPS_PY_OK" = 1 ]; then
  "$OPS_PY" -m pytest services/ops-checker --junitxml="$TMP/results/main/py/ops-checker.xml"
  rc=$?
  if [ "$rc" = 0 ]; then stage opschecker "확인기" ok "" $((SECONDS - t)) main/py/ops-checker.xml
  else stage opschecker "확인기" fail "pytest 종료 코드 $rc" $((SECONDS - t)) main/py/ops-checker.xml; fi
else
  stage opschecker "확인기" fail "확인기 venv 파이썬이 없음(TEST_ALL_OPS_PYTHON, scripts/setup-dev.sh)" 0
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

log "5. 실제 DB 테스트"
t=$SECONDS
if [ "$DB_READY" = 1 ]; then
  if run_db_tests; then stage db "실제 DB 테스트" ok "" $((SECONDS - t)) db
  else stage db "실제 DB 테스트" fail "" $((SECONDS - t)) db; fi
else
  stage db "실제 DB 테스트" fail "DB 준비 실패로 돌리지 못함" 0
fi

# ---------------------------------------------------------------- 5b. 구성 종목 리허설 (가짜 운용사 서버)

log "5b. 구성 종목 리허설"
t=$SECONDS
if [ "$DB_READY" = 0 ]; then
  stage members "구성 종목 리허설" fail "DB 준비 실패로 돌리지 못함" 0
elif run_members_rehearsal; then
  stage members "구성 종목 리허설" ok "" $((SECONDS - t)) main/py/scripts-members.xml
else
  echo "구성 종목 리허설 실패: $MEMBERS_FAIL_REASON"
  stage members "구성 종목 리허설" fail "$MEMBERS_FAIL_REASON" $((SECONDS - t)) main/py/scripts-members.xml
fi

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
M_PW="" N_PW="" B_PW="" PO_PW="" PW_PW="" PC_PW="" PN_PW=""

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
