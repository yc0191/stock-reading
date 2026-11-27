# shellcheck shell=bash
# test-all.sh 가 source 하는 실제 DB 준비(4단계)와 실제 DB 테스트(5단계) 함수. 직접 실행하지 않는다.
# 쓰는 변수: ROOT, PG_CONTAINER, DB_NAME, ENV_FILE, PY, OPS_PY, TMP 와 scripts/personal-db-lib.sh 의 함수.

# ---------------------------------------------------------------- 4. 실제 DB 준비

M_USER="" M_PW="" N_USER="" N_PW="" B_USER="" B_PW=""
PO_USER="" PO_PW="" PW_USER="" PW_PW="" PC_USER="" PC_PW="" PN_USER="" PN_PW=""
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
  for key in MARKET_DB_USER MARKET_DB_PASSWORD NEWS_DB_USER NEWS_DB_PASSWORD BRIEFING_DB_USER BRIEFING_DB_PASSWORD \
      PERSONAL_OWNER_DB_USER PERSONAL_OWNER_DB_PASSWORD PERSONAL_WRITER_DB_USER PERSONAL_WRITER_DB_PASSWORD \
      COLLECTOR_DB_USER COLLECTOR_DB_PASSWORD NOTIFIER_DB_USER NOTIFIER_DB_PASSWORD OPS_CHECKER_DB_USER OPS_CHECKER_DB_PASSWORD; do
    if [ -z "$(dotenv_value "$key")" ]; then DB_FAIL_REASON=".env 에 $key 가 없음"; return 1; fi
  done
  M_USER=$(dotenv_value MARKET_DB_USER); M_PW=$(dotenv_value MARKET_DB_PASSWORD)
  N_USER=$(dotenv_value NEWS_DB_USER); N_PW=$(dotenv_value NEWS_DB_PASSWORD)
  B_USER=$(dotenv_value BRIEFING_DB_USER); B_PW=$(dotenv_value BRIEFING_DB_PASSWORD)
  PO_USER=$(dotenv_value PERSONAL_OWNER_DB_USER); PO_PW=$(dotenv_value PERSONAL_OWNER_DB_PASSWORD)
  PW_USER=$(dotenv_value PERSONAL_WRITER_DB_USER); PW_PW=$(dotenv_value PERSONAL_WRITER_DB_PASSWORD)
  PC_USER=$(dotenv_value COLLECTOR_DB_USER); PC_PW=$(dotenv_value COLLECTOR_DB_PASSWORD)
  OPS_USER=$(dotenv_value OPS_CHECKER_DB_USER); OPS_PW=$(dotenv_value OPS_CHECKER_DB_PASSWORD)
  PN_USER=$(dotenv_value NOTIFIER_DB_USER); PN_PW=$(dotenv_value NOTIFIER_DB_PASSWORD)
  for role in "$M_USER" "$N_USER" "$B_USER" "$PO_USER" "$PW_USER" "$PC_USER" "$PN_USER"; do
    # SQL 에 그대로 넣으므로 이름 형식을 먼저 확인한다.
    if ! [[ "$role" =~ ^[a-z_][a-z0-9_]*$ ]]; then DB_FAIL_REASON=".env 의 서비스 계정 이름 형식이 맞지 않음"; return 1; fi
    found=$(printf "SELECT 1 FROM pg_roles WHERE rolname = '%s';\n" "$role" | admin_sql 2>/dev/null)
    if [ "$found" != 1 ]; then DB_FAIL_REASON="서비스 계정 $role 이 없음(계정은 만들지 않음, docs/dev/setup.md 참고: personal 계정은 infra/db/personal/init.sql 로 만듦)"; return 1; fi
  done
  # 확인기 계정은 init_ops 가 없으면 만든다(infra/db/ops/init.sql). SQL 에 그대로 넣으므로 이름 형식만 먼저 확인한다.
  if ! [[ "$OPS_USER" =~ ^[a-z_][a-z0-9_]*$ ]]; then DB_FAIL_REASON=".env 의 확인기 계정 이름 형식이 맞지 않음"; return 1; fi
  warn_leftovers
  if ! printf 'CREATE DATABASE "%s";\n' "$DB_NAME" | admin_sql >/dev/null; then
    DB_FAIL_REASON="데이터베이스 $DB_NAME 를 만들지 못함"; return 1
  fi
  DB_CREATED=1
  if ! docker exec -i "$PG_CONTAINER" sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"' sh "$DB_NAME" >/dev/null <<SQL
CREATE SCHEMA market AUTHORIZATION "$M_USER";
CREATE SCHEMA news AUTHORIZATION "$N_USER";
CREATE SCHEMA briefing AUTHORIZATION "$B_USER";
-- 옛 표 이관(market V10) 테스트가 쓰는 별도 스키마. 이름은 MARKET_IT_DB_MIGRATION_SCHEMA 로 넘긴다.
CREATE SCHEMA market_migrate_it AUTHORIZATION "$M_USER";
-- 기사-재검색 작업 연결 이관(news V9) 테스트가 쓰는 별도 스키마. 이름은 NEWS_IT_DB_MIGRATION_SCHEMA 로 넘긴다.
CREATE SCHEMA news_migrate_it AUTHORIZATION "$N_USER";
SQL
  then
    DB_FAIL_REASON="서비스 스키마를 만들지 못함"; return 1
  fi
  if ! migrate_market; then DB_FAIL_REASON="market 표 Flyway 적용 실패(docker 이미지 $FLYWAY_IMAGE)"; return 1; fi
  if ! init_personal; then DB_FAIL_REASON="personal 계정·권한 스크립트 실패(infra/db/personal/init.sql)"; return 1; fi
  if ! migrate_personal; then DB_FAIL_REASON="personal 표 Flyway 적용 실패(docker 이미지 $FLYWAY_IMAGE)"; return 1; fi
  if ! migrate_briefing; then DB_FAIL_REASON="브리핑 테이블 Flyway 적용 실패(docker 이미지 $FLYWAY_IMAGE)"; return 1; fi
  if ! init_ops; then DB_FAIL_REASON="ops 계정·권한 스크립트 실패(infra/db/ops/init.sql)"; return 1; fi
  if ! migrate_ops; then DB_FAIL_REASON="ops 표 Flyway 적용 실패(docker 이미지 $FLYWAY_IMAGE)"; return 1; fi
}

# ---------------------------------------------------------------- 5. 실제 DB 테스트

JDBC_URL="jdbc:postgresql://127.0.0.1:5432/$DB_NAME"
PG_URL="postgresql://127.0.0.1:5432/$DB_NAME"

run_db_tests() {
  local line group names name gradle_args="" serial_args="" java_groups="" briefing_files="" scripts_files="" ops_files="" rc=0 path
  # 지도(docs/test-map)에서 켜는 조건이 *_IT_DB_* 인 테스트를 고른다. 새 실제 DB 테스트를 지도에 올리면 여기에 들어온다.
  while read -r group names; do
    [ -n "$group" ] || continue
    case "$group" in
      market)
        java_groups="$java_groups $group"
        gradle_args="$gradle_args :services:$group:cleanTest :services:$group:test"
        for name in $names; do gradle_args="$gradle_args --tests $name"; done ;;
      news|collector|notifier)
        # 같은 테스트 DB 의 market·personal 표를 market 테스트와 함께 쓰므로 market 이 끝난 뒤 서비스마다 따로 돌린다(gradle 이 프로젝트를 병렬로 돌리기 때문).
        java_groups="$java_groups $group"
        serial_args="$serial_args|:services:$group:cleanTest :services:$group:test"
        for name in $names; do serial_args="$serial_args --tests $name"; done ;;
      briefing)
        for name in $names; do
          path=$(find services/briefing/tests -name "$name" -o -name "$name.py" | head -1)
          [ -n "$path" ] && briefing_files="$briefing_files $path"
        done ;;
      ops-checker)
        for name in $names; do
          path=$(find services/ops-checker/tests -name "$name" -o -name "$name.py" | head -1)
          [ -n "$path" ] && ops_files="$ops_files $path"
        done ;;
      scripts)
        for name in $names; do
          path=$(find scripts/tests -name "$name" -o -name "$name.py" | head -1)
          [ -n "$path" ] && scripts_files="$scripts_files $path"
        done ;;
      *) echo "실제 DB 테스트를 돌리는 방법이 없는 지도: $group"; rc=1 ;;
    esac
  done < <(python3 scripts/run_report.py db-tests)
  if [ -z "$java_groups$briefing_files$scripts_files$ops_files" ]; then echo "지도에서 실제 DB 테스트를 찾지 못했습니다"; return 1; fi
  if [ -n "$java_groups" ]; then
    if [ "$JAVA_OK" = 1 ]; then
      # shellcheck disable=SC2086
      (
        export MARKET_IT_DB_URL="$JDBC_URL" MARKET_IT_DB_USER="$M_USER" MARKET_IT_DB_PASSWORD="$M_PW"
        export MARKET_IT_DB_MIGRATION_SCHEMA=market_migrate_it
        export NEWS_IT_DB_URL="$JDBC_URL" NEWS_IT_DB_USER="$N_USER" NEWS_IT_DB_PASSWORD="$N_PW"
        export NEWS_IT_DB_MIGRATION_SCHEMA=news_migrate_it
        export WRITER_IT_DB_USER="$PW_USER" WRITER_IT_DB_PASSWORD="$PW_PW"
        export COLLECTOR_IT_DB_USER="$PC_USER" COLLECTOR_IT_DB_PASSWORD="$PC_PW"
        export COLLECTOR_IT_DB_URL="$JDBC_URL"
        export NOTIFIER_IT_DB_URL="$JDBC_URL" NOTIFIER_IT_DB_USER="$PN_USER" NOTIFIER_IT_DB_PASSWORD="$PN_PW"
        if [ -n "$gradle_args" ]; then ./gradlew $gradle_args --no-build-cache || exit 1; fi
        run_args=""
        IFS='|' read -r -a serial_runs <<<"${serial_args#|}"
        for run_args in "${serial_runs[@]}"; do
          [ -n "$run_args" ] && { ./gradlew $run_args --no-build-cache || exit 1; }
        done
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
        # 멤버 읽기 테스트는 personal 쓰기 계정과 market 주인 계정으로 값을 넣는다(브리핑 계정은 읽기만 한다).
        export WRITER_IT_DB_USER="$PW_USER" WRITER_IT_DB_PASSWORD="$PW_PW" MARKET_IT_DB_USER="$M_USER" MARKET_IT_DB_PASSWORD="$M_PW"
        "$PY" -m pytest $briefing_files --junitxml="$TMP/results/db/py/briefing.xml"
      ) || rc=1
    else
      rc=1
    fi
  fi
  if [ -n "$ops_files" ]; then
    if [ "$OPS_PY_OK" = 1 ]; then
      # 확인기 계정 하나로 읽기 전용 실행과 장부 쓰기를 확인한다(ops 표는 init_ops·migrate_ops 가 이미 만들었다).
      # shellcheck disable=SC2086
      (
        export OPS_IT_DB_URL="$PG_URL" OPS_IT_DB_USER="$OPS_USER" OPS_IT_DB_PASSWORD="$OPS_PW"
        "$OPS_PY" -m pytest $ops_files --junitxml="$TMP/results/db/py/ops-checker.xml"
      ) || rc=1
    else
      echo "확인기 venv 파이썬이 없어 확인기 실제 DB 테스트를 못 돌림(TEST_ALL_OPS_PYTHON, scripts/setup-dev.sh)"
      rc=1
    fi
  fi
  if [ -n "$scripts_files" ]; then
    if [ "$PY_OK" = 1 ]; then
      # 확인기 계정의 권한 테스트는 확인기 계정으로 읽고, market·briefing 주인 계정으로 새 표를 만들어 본다.
      # shellcheck disable=SC2086
      (
        export OPS_IT_DB_URL="$PG_URL" OPS_IT_DB_USER="$OPS_USER" OPS_IT_DB_PASSWORD="$OPS_PW"
        export MARKET_IT_DB_USER="$M_USER" MARKET_IT_DB_PASSWORD="$M_PW"
        export BRIEFING_IT_DB_USER="$B_USER" BRIEFING_IT_DB_PASSWORD="$B_PW"
        "$PY" -m pytest -c scripts/pytest.ini $scripts_files --junitxml="$TMP/results/db/py/scripts-ops.xml"
      ) || rc=1
    else
      rc=1
    fi
  fi
  return "$rc"
}
