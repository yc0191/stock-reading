# test-all.sh 와 manual-daily-briefing.sh 가 함께 쓰는 DB 준비 함수. 직접 실행하지 않고 source 한다.
# 쓰는 변수: ROOT, PG_CONTAINER, DB_NAME, FLYWAY_IMAGE, M_USER, M_PW(시세처리기), N_USER(뉴스), B_USER(브리핑), PO_*(personal 소유), PW_*(쓰기), PC_*(수집기), PN_*(발송기).
# 순서: market 표(migrate_market) → personal 계정·권한(init_personal) → personal 표(migrate_personal).
# personal 표가 market.etf·market.stock 을 가리키는 외래 키를 가지므로 이 순서를 지킨다.

# market 표를 Flyway 로 만든다. personal 이 market.etf·market.stock 을 가리키므로 personal 보다 먼저 한다.
# schema 를 주면(두 번째 인자) 그 스키마에 target 버전(세 번째 인자)까지만 적용한다(옛 표 이관 테스트용).
migrate_market() {
  local schema=${1:-market} target=${2:-} out
  if ! out=$(FLYWAY_PASSWORD="$M_PW" docker run --rm --network "container:$PG_CONTAINER" \
      -v "$ROOT/services/market/src/main/resources/db/migration:/flyway/sql:ro" \
      -e FLYWAY_PASSWORD -e FLYWAY_USER="$M_USER" -e FLYWAY_URL="jdbc:postgresql://localhost:5432/$DB_NAME" \
      -e FLYWAY_SCHEMAS="$schema" -e FLYWAY_DEFAULT_SCHEMA="$schema" -e FLYWAY_CREATE_SCHEMAS=false \
      -e FLYWAY_LOCATIONS=filesystem:/flyway/sql ${target:+-e FLYWAY_TARGET="$target"} "$FLYWAY_IMAGE" migrate 2>&1); then
    printf '%s\n' "$out" | tail -15
    return 1
  fi
  printf '%s\n' "$out" | grep -E 'Successfully applied|up to date' | head -2
}

# personal 계정·권한을 만든다(infra/db/personal/init.sql). 계정은 .env 에 있는 이미 만들어진 것이고, 비밀번호는 환경변수로만 넘긴다.
init_personal() {
  PERSONAL_OWNER_DB_USER="$PO_USER" PERSONAL_OWNER_DB_PASSWORD="$PO_PW" PERSONAL_WRITER_DB_USER="$PW_USER" \
  PERSONAL_WRITER_DB_PASSWORD="$PW_PW" COLLECTOR_DB_USER="$PC_USER" COLLECTOR_DB_PASSWORD="$PC_PW" \
  NOTIFIER_DB_USER="$PN_USER" NOTIFIER_DB_PASSWORD="$PN_PW" MARKET_DB_USER="$M_USER" NEWS_DB_USER="$N_USER" BRIEFING_DB_USER="$B_USER" \
    docker exec -i -e PERSONAL_OWNER_DB_USER -e PERSONAL_OWNER_DB_PASSWORD -e PERSONAL_WRITER_DB_USER \
      -e PERSONAL_WRITER_DB_PASSWORD -e COLLECTOR_DB_USER -e COLLECTOR_DB_PASSWORD -e NOTIFIER_DB_USER \
      -e NOTIFIER_DB_PASSWORD -e MARKET_DB_USER -e NEWS_DB_USER -e BRIEFING_DB_USER "$PG_CONTAINER" \
      sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1" -f -' sh "$DB_NAME" < "$ROOT/infra/db/personal/init.sql" >/dev/null
}

migrate_personal() {
  local out
  if ! out=$(FLYWAY_PASSWORD="$PO_PW" docker run --rm --network "container:$PG_CONTAINER" \
      -v "$ROOT/infra/db/personal/migration:/flyway/sql:ro" \
      -e FLYWAY_PASSWORD -e FLYWAY_USER="$PO_USER" -e FLYWAY_URL="jdbc:postgresql://localhost:5432/$DB_NAME" \
      -e FLYWAY_SCHEMAS=personal -e FLYWAY_DEFAULT_SCHEMA=personal -e FLYWAY_CREATE_SCHEMAS=false \
      -e FLYWAY_LOCATIONS=filesystem:/flyway/sql "$FLYWAY_IMAGE" migrate \
      "-placeholders.market_user=$M_USER" "-placeholders.collector_user=$PC_USER" \
      "-placeholders.notifier_user=$PN_USER" "-placeholders.writer_user=$PW_USER" \
      "-placeholders.briefing_user=$B_USER" 2>&1); then
    printf '%s\n' "$out" | tail -15
    return 1
  fi
  printf '%s\n' "$out" | grep -E 'Successfully applied|up to date' | head -2
}

