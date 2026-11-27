# shellcheck shell=bash
# test-all.sh 가 source 하는 구성 종목 리허설(5b단계, 가짜 운용사 서버) 함수. 직접 실행하지 않는다.

# 수집기가 가짜 운용사 사이트에서 구성 종목을 받아 etf.members.fetched 로 넘기고, 시세처리기가 테스트 전용 데이터베이스에
# 원문 저장·비교·반영하는지 끝까지 돌린다. 외부는 부르지 않는다. 시세 흐름 실제 단계가 같은 이름의 Kafka 를 다시 띄우므로
# 이 단계가 띄운 것(Kafka, 두 서비스, 가짜 서버)은 단계가 끝날 때 바로 정리한다.

MEMBERS_FAIL_REASON=""

seed_members_rehearsal() {
  # 리허설 ETF 는 가짜 종목코드(RHRS 접두어)이며 출처 번호는 가짜 서버가 아는 값이다(scripts/fake_fund_site.py).
  docker exec -i "$PG_CONTAINER" sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"' sh "$DB_NAME" >/dev/null <<SQL
INSERT INTO market.etf (market, symbol, name, constituent_source_id) VALUES
  ('KR', 'RHRS01', 'REHEARSAL ETF OK', 'RHRS-OK'), ('KR', 'RHRS02', 'REHEARSAL ETF DOWN', 'RHRS-DOWN')
ON CONFLICT (market, symbol) DO NOTHING;
SQL
}

members_rehearsal_steps() {
  local fund_port svc_dir="$TMP/svc-members" request_log
  if [ "$PY_OK" = 0 ] || [ "$JAVA_OK" = 0 ]; then MEMBERS_FAIL_REASON="JDK 21 또는 브리핑 venv 가 없음"; return 1; fi
  echo "bootJar 빌드(market, collector)"
  ./gradlew -q :services:collector:bootJar :services:market:bootJar || { MEMBERS_FAIL_REASON="bootJar 빌드 실패"; return 1; }
  MARKET_JAR=$(service_jar market)
  COLLECTOR_JAR=$(service_jar collector)
  if [ -z "$MARKET_JAR" ] || [ -z "$COLLECTOR_JAR" ]; then MEMBERS_FAIL_REASON="bootJar 를 찾지 못함"; return 1; fi
  start_kafka || { MEMBERS_FAIL_REASON="$KAFKA_FAIL_REASON"; return 1; }

  mkdir -p "$svc_dir/market" "$svc_dir/collector"
  request_log="$svc_dir/fund-requests.log"
  : > "$request_log"
  fund_port=$(free_port)
  echo "가짜 운용사 서버 기동(127.0.0.1:$fund_port)"
  "$PY" "$ROOT/scripts/fake_fund_site.py" "$fund_port" "$request_log" > "$svc_dir/fake-fund.log" 2>&1 &
  FAKE_FUND_PID=$!
  seed_members_rehearsal || { MEMBERS_FAIL_REASON="리허설 ETF 를 넣지 못함"; return 1; }

  echo "market 기동(테스트 전용 데이터베이스)"
  (
    export SPRING_DATASOURCE_URL="$JDBC_URL?currentSchema=market" SPRING_DATASOURCE_USERNAME="$M_USER" SPRING_DATASOURCE_PASSWORD="$M_PW"
    # 내부 조회 API 는 이 단계에서 쓰지 않지만 기본 포트가 개발용 시세처리기와 겹치지 않게 빈 포트로 띄운다.
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:$KAFKA_PORT" SERVER_PORT="$(free_port)"
    cd "$svc_dir/market" && exec "$JAVA_HOME_21/bin/java" -jar "$MARKET_JAR"
  ) > "$svc_dir/market.log" 2>&1 &
  MARKET_PID=$!
  wait_for_log "$svc_dir/market.log" "groupId=market-members] Adding newly assigned partitions:" "$MARKET_PID" 120
  case $? in
    0) ;;
    2) MEMBERS_FAIL_REASON="market 이 시작 중에 끝남"; return 1 ;;
    *) MEMBERS_FAIL_REASON="market 이 120초 안에 준비되지 않음"; return 1 ;;
  esac

  echo "collector 기동(가짜 운용사 서버, 타이머 모두 끔)"
  (
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:$KAFKA_PORT"
    export SPRING_DATASOURCE_URL="$JDBC_URL" SPRING_DATASOURCE_USERNAME="$PC_USER" SPRING_DATASOURCE_PASSWORD="$PC_PW"
    export MEMBERS_URL_TEMPLATE="http://127.0.0.1:$fund_port/pdf/{fundId}?base={date}" MEMBERS_USER_AGENT=stock-reading-rehearsal/1.0
    # 타이머를 모두 꺼서 구성 종목 조회는 아래 수동 실행 명령 1건뿐이게 한다. 시세는 기본(시뮬레이터)이라 외부를 부르지 않는다.
    export COLLECTOR_SCHEDULE_ENABLED=false COLLECTOR_PRE_OPEN_ENABLED=false COLLECTOR_INSIGHT_SCHEDULE_ENABLED=false
    export COLLECTOR_INTRADAY_ENABLED=false COLLECTOR_MEMBERS_ENABLED=false COLLECTOR_DAILY_BRIEFING_ENABLED=false
    export COLLECTOR_MARKET_DATA_SOURCE=simulator
    cd "$svc_dir/collector" && exec "$JAVA_HOME_21/bin/java" -jar "$COLLECTOR_JAR"
  ) > "$svc_dir/collector.log" 2>&1 &
  COLLECTOR_PID=$!
  # 명령 리스너는 파티션을 받을 때 끝으로 옮기므로(꺼져 있던 동안의 명령은 실행하지 않음) 받기 전에 보낸 명령은 놓친다.
  wait_for_log "$svc_dir/collector.log" "groupId=collector-command] Adding newly assigned partitions: [collector.command" "$COLLECTOR_PID" 120
  case $? in
    0) ;;
    2) MEMBERS_FAIL_REASON="collector 가 시작 중에 끝남"; return 1 ;;
    *) MEMBERS_FAIL_REASON="collector 가 120초 안에 준비되지 않음"; return 1 ;;
  esac
  sleep 3

  echo "수동 실행 명령 collect-etf-members 1건 발행"
  KAFKA_CONTAINER="$KAFKA_NAME" KAFKA_BOOTSTRAP="localhost:$KAFKA_PORT" bash scripts/collector-command.sh collect-etf-members >/dev/null \
    || { MEMBERS_FAIL_REASON="수동 실행 명령을 보내지 못함"; return 1; }

  echo "테스트 전용 데이터베이스에서 다시 읽어 확인"
  (
    export MEMBERS_FLOW_DB_URL="$PG_URL" MEMBERS_FLOW_DB_USER="$M_USER" MEMBERS_FLOW_DB_PASSWORD="$M_PW"
    export MEMBERS_FLOW_REQUEST_LOG="$request_log" MEMBERS_FLOW_WAIT_SECONDS=60
    "$PY" -m pytest -c scripts/pytest.ini scripts/tests/test_members_flow_rehearsal.py --junitxml="$TMP/results/main/py/scripts-members.xml"
  ) || { MEMBERS_FAIL_REASON="반영 확인 실패(서비스 로그는 $svc_dir, 끝나면 지워짐)"; return 1; }
}

run_members_rehearsal() {
  local rc=0
  members_rehearsal_steps || rc=1
  # 이 단계가 띄운 것만 바로 정리한다(데이터베이스는 마지막 정리 단계가 지운다).
  stop_service "$COLLECTOR_PID" "$COLLECTOR_JAR" collector || rc=1
  COLLECTOR_PID=""
  stop_service "$MARKET_PID" "$MARKET_JAR" market || rc=1
  MARKET_PID=""
  stop_service "$FAKE_FUND_PID" "$ROOT/scripts/fake_fund_site.py" "가짜 운용사 서버" || rc=1
  FAKE_FUND_PID=""
  remove_kafka || rc=1
  if [ "$rc" = 1 ] && [ -z "$MEMBERS_FAIL_REASON" ]; then MEMBERS_FAIL_REASON="정리하지 못한 것이 있음(위 안내 참고)"; fi
  return "$rc"
}
