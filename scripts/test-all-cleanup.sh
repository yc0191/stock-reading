# shellcheck shell=bash
# test-all.sh 가 source 하는 정리 함수(이 실행이 만든 데이터베이스·컨테이너·프로세스·임시 폴더만 지움). 직접 실행하지 않는다.
# 쓰는 변수: test-all.sh 맨 위의 실행 이름·상태 변수(DB_CREATED, KAFKA_STARTED, *_PID, TMP, LOCKED 등).

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
  stop_service "$FAKE_FUND_PID" "$ROOT/scripts/fake_fund_site.py" "가짜 운용사 서버" || bad=1
  FAKE_FUND_PID=""
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
