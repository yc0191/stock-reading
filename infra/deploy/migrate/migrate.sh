#!/bin/bash
# 서비스 표와 계정 권한을 순서대로 맞춘다. 여러 번 실행해도 같다(Flyway 는 적용한 파일을 건너뛰고, init.sql 은 이미 있는 것을 그대로 둔다).
# 순서: market 표 -> personal 계정·권한(market 표를 가리킴) -> personal 표 -> briefing 표. 로컬 Compose 의 *-migrate 단계와 같다.
# 비밀번호는 환경변수로만 받고 명령줄에 쓰지 않는다.
set -euo pipefail

: "${DB_HOST:?DB_HOST 가 필요합니다}"
db="${POSTGRES_DB:?POSTGRES_DB 가 필요합니다}"
url="jdbc:postgresql://${DB_HOST}:${DB_PORT:-5432}/${db}"

# 한 스키마의 Flyway 를 그 스키마 주인 계정으로 돌린다. 인자: 스키마 계정 비밀번호 폴더 [추가 Flyway 인자...]
flyway_migrate() {
  local schema=$1 user=$2 password=$3 dir=$4
  shift 4
  echo "Flyway: ${schema}"
  FLYWAY_PASSWORD="$password" /flyway/flyway -url="$url" -user="$user" \
    -schemas="$schema" -defaultSchema="$schema" -createSchemas=false \
    -locations="filesystem:${dir}" "$@" migrate
}

flyway_migrate market "$MARKET_DB_USER" "$MARKET_DB_PASSWORD" /sql/market

echo "personal 계정·권한"
PGHOST="$DB_HOST" PGPORT="${DB_PORT:-5432}" PGDATABASE="$db" PGUSER="$POSTGRES_ADMIN_USER" PGPASSWORD="$POSTGRES_ADMIN_PASSWORD" \
  psql -v ON_ERROR_STOP=1 -f /sql/personal-init.sql

flyway_migrate personal "$PERSONAL_OWNER_DB_USER" "$PERSONAL_OWNER_DB_PASSWORD" /sql/personal \
  "-placeholders.market_user=${MARKET_DB_USER}" "-placeholders.collector_user=${COLLECTOR_DB_USER}" \
  "-placeholders.notifier_user=${NOTIFIER_DB_USER}" "-placeholders.writer_user=${PERSONAL_WRITER_DB_USER}" \
  "-placeholders.briefing_user=${BRIEFING_DB_USER}"

flyway_migrate briefing "$BRIEFING_DB_USER" "$BRIEFING_DB_PASSWORD" /sql/briefing
