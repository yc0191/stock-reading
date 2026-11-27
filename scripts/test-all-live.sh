# shellcheck shell=bash
# test-all.sh 가 source 하는 실제 호출(--live) 함수: 시세 흐름 실제(6단계)와 브리핑·메일 실제(6b단계). 직접 실행하지 않는다.
# 실제 호출 키는 실행할 때마다 Parameter Store 에서 읽는다(머리 주석은 test-all.sh 참고).

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
LIVE_KEYS="BROKER_BASE_URL BROKER_APP_KEY BROKER_APP_SECRET TEST_ALL_LIVE_SYMBOL ANTHROPIC_API_KEY SMTP_HOST SMTP_USERNAME SMTP_PASSWORD TEST_ALL_LIVE_RECIPIENT NEWS_SEARCH_CLIENT_ID NEWS_SEARCH_CLIENT_SECRET"

missing_live_keys() {
  local key missing=""
  for key in $LIVE_KEYS; do
    [ -n "$(live_value "$key")" ] || missing="$missing${missing:+·}$key"
  done
  printf '%s' "$missing"
}

# 비밀이 아닌 뉴스 검색 설정(제공자 주소, 인증 헤더 이름)은 Parameter Store 에 두지 않으므로 실행 환경변수나 .env 에서 읽는다.
missing_live_settings() {
  local missing=""
  [ -n "$(env_or_dotenv NEWS_SEARCH_BASE_URL)" ] || missing="$missing${missing:+·}NEWS_SEARCH_BASE_URL"
  [ -n "$(env_or_dotenv NEWS_SEARCH_ID_HEADER)" ] || missing="$missing${missing:+·}NEWS_SEARCH_ID_HEADER"
  [ -n "$(env_or_dotenv NEWS_SEARCH_SECRET_HEADER)" ] || missing="$missing${missing:+·}NEWS_SEARCH_SECRET_HEADER"
  printf '%s' "$missing"
}

KAFKA_FAIL_REASON=""

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

# 이 실행만의 일회용 Kafka 를 띄운다(라벨로 구분, 정리는 remove_kafka). 포트는 KAFKA_PORT 에 둔다.
start_kafka() {
  local port waited
  port=$(free_port)
  KAFKA_PORT=$port
  echo "일회용 Kafka 기동: $KAFKA_NAME (127.0.0.1:$port)"
  KAFKA_STARTED=1
  # 컨테이너 안팎에서 같은 포트를 써서 collector-command.sh 도 같은 주소로 붙게 한다. 볼륨이 없어 지우면 데이터도 사라진다.
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
    KAFKA_FAIL_REASON="일회용 Kafka 를 띄우지 못함"; return 1
  fi
  waited=0
  until docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "localhost:$port" >/dev/null 2>&1; do
    if [ "$waited" -ge 60 ]; then KAFKA_FAIL_REASON="일회용 Kafka 가 60초 안에 준비되지 않음"; return 1; fi
    sleep 2; waited=$((waited + 2))
  done
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
  missing=$(missing_live_settings)
  if [ -n "$missing" ]; then LIVE_FAIL_REASON="설정 없음($missing, 실행 환경변수나 .env)"; return 1; fi
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

  start_kafka || { LIVE_FAIL_REASON="$KAFKA_FAIL_REASON"; return 1; }
  port=$KAFKA_PORT

  # 작업 위치를 임시 폴더 안쪽으로 두어 market 이 저장소의 .env 를 읽지 않게 한다.
  svc_dir="$TMP/svc"
  mkdir -p "$svc_dir/market" "$svc_dir/collector"
  # 시세처리기 내부 조회 API 포트도 빈 포트로 둔다. 기본 포트(8091)를 쓰면 개발용 시세처리기와 겹치고, 브리핑이 그쪽을 부를 수 있다.
  MARKET_HTTP_PORT=$(free_port)
  echo "market 기동(테스트 전용 데이터베이스, 내부 조회 127.0.0.1:$MARKET_HTTP_PORT)"
  # MARKET_PREV_CLOSE_FIELDS 를 빈 값으로 넘겨 전일 종가 기준 감지를 끈다. 스냅샷이 1개라 구간 감지도 일어나지 않으므로
  # market 이 스스로 변동을 내지 않고, 브리핑·메일 단계의 변동은 스크립트가 넣은 1건뿐이다.
  (
    export MARKET_PREV_CLOSE_FIELDS=
    export SPRING_DATASOURCE_URL="$JDBC_URL?currentSchema=market" SPRING_DATASOURCE_USERNAME="$M_USER" SPRING_DATASOURCE_PASSWORD="$M_PW"
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:$port" SERVER_PORT="$MARKET_HTTP_PORT"
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

  # 수집 대상은 DB 에서 읽으므로(재시작 때 반영) 수집기를 띄우기 전에 테스트 멤버의 포트폴리오에 시세 종목 1개를 넣는다.
  echo "테스트 멤버의 포트폴리오에 종목 1개 넣기(수집 대상은 DB 에서 읽음)"
  seed_live_portfolio || { LIVE_FAIL_REASON="테스트 멤버 포트폴리오를 넣지 못함"; return 1; }
  echo "collector 기동(실제 시세, 종목 1개)"
  (
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:$port"
    export SPRING_DATASOURCE_URL="$JDBC_URL" SPRING_DATASOURCE_USERNAME="$PC_USER" SPRING_DATASOURCE_PASSWORD="$PC_PW"
    export COLLECTOR_MARKET_DATA_SOURCE=broker BROKER_BASE_URL="$base_url" BROKER_APP_KEY="$app_key" BROKER_APP_SECRET="$app_secret"
    # 브리핑이 근거 약한 문장을 재검색하면 이 수집기가 실제 뉴스 검색으로 답한다. 키가 없으면 다 모으지 못함으로 끝나 브리핑 확인이 실패한다.
    export NEWS_SEARCH_CLIENT_ID="$(live_value NEWS_SEARCH_CLIENT_ID)" NEWS_SEARCH_CLIENT_SECRET="$(live_value NEWS_SEARCH_CLIENT_SECRET)"
    export NEWS_SEARCH_BASE_URL="$(env_or_dotenv NEWS_SEARCH_BASE_URL)" NEWS_SEARCH_ID_HEADER="$(env_or_dotenv NEWS_SEARCH_ID_HEADER)"
    export NEWS_SEARCH_SECRET_HEADER="$(env_or_dotenv NEWS_SEARCH_SECRET_HEADER)"
    # 타이머를 모두 꺼서 시세 호출은 아래 수동 실행 명령 1건뿐이게 한다.
    export COLLECTOR_SCHEDULE_ENABLED=false COLLECTOR_PRE_OPEN_ENABLED=false COLLECTOR_INSIGHT_SCHEDULE_ENABLED=false
    export COLLECTOR_INTRADAY_ENABLED=false COLLECTOR_MEMBERS_ENABLED=false COLLECTOR_DAILY_BRIEFING_ENABLED=false
    cd "$svc_dir/collector" && exec "$JAVA_HOME_21/bin/java" -jar "$COLLECTOR_JAR"
  ) > "$svc_dir/collector.log" 2>&1 &
  COLLECTOR_PID=$!
  # 명령 리스너는 파티션을 받을 때 끝으로 옮기므로(꺼져 있던 동안의 명령은 실행하지 않음) 받기 전에 보낸 명령은 놓친다.
  wait_for_log "$svc_dir/collector.log" "groupId=collector-command] Adding newly assigned partitions: [collector.command" "$COLLECTOR_PID" 120
  case $? in
    0) ;;
    2) LIVE_FAIL_REASON="collector 가 시작 중에 끝남"; return 1 ;;
    *) LIVE_FAIL_REASON="collector 가 120초 안에 준비되지 않음"; return 1 ;;
  esac
  sleep 3

  # 같은 테스트 DB 에 실제 DB 테스트가 넣은 가짜 행이 남아 있어, 이 시각 뒤에 저장된 행만 이번 실행의 결과로 센다.
  LIVE_SINCE=$(echo "SELECT now();" | admin_sql) || { LIVE_FAIL_REASON="DB 시각을 읽지 못함"; return 1; }
  echo "수동 실행 명령 collect 1건 발행"
  KAFKA_CONTAINER="$KAFKA_NAME" KAFKA_BOOTSTRAP="localhost:$port" bash scripts/collector-command.sh collect >/dev/null \
    || { LIVE_FAIL_REASON="수동 실행 명령을 보내지 못함"; return 1; }

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

# 시세 흐름 단계가 쓰는 종목(TEST_ALL_LIVE_SYMBOL)을 ETF 로 넣고, 테스트 멤버가 그 종목을 포트폴리오에 담게 한다.
# 수집기가 이 포트폴리오(수집 대상 뷰)에서 1초 시세 종목을 읽는다.
seed_live_portfolio() {
  local symbol
  symbol=$(live_value TEST_ALL_LIVE_SYMBOL)
  [[ "$symbol" =~ ^[0-9]{6}$ ]] || return 1
  docker exec -i "$PG_CONTAINER" sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"' sh "$DB_NAME" >/dev/null <<SQL
INSERT INTO market.etf (market, symbol, name) VALUES ('KR', '$symbol', 'TEST LIVE ETF') ON CONFLICT (market, symbol) DO NOTHING;
INSERT INTO personal.member (name) VALUES ('TEST LIVE MEMBER');
INSERT INTO personal.member_portfolio (member_id, etf_id, relation)
SELECT (SELECT max(id) FROM personal.member WHERE name = 'TEST LIVE MEMBER'), id, 'HELD'
FROM market.etf WHERE symbol = '$symbol' AND market = 'KR';
SQL
}

# 테스트 멤버에게 받는 주소를 더한다(받는 사람은 발송기가 DB 에서 고름).
seed_live_member() {
  local address
  address=$(live_value TEST_ALL_LIVE_RECIPIENT)
  # 주소는 명령줄이 아니라 환경변수로 넘긴다(\getenv). SQL 에 넣기 전에 따옴표·공백이 없는 주소 모양인지 확인한다.
  [[ "$address" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+$ ]] || return 1
  LIVE_MEMBER_ADDRESS="$address" docker exec -i -e LIVE_MEMBER_ADDRESS "$PG_CONTAINER" \
    sh -c 'psql -qAtX -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"' sh "$DB_NAME" >/dev/null <<SQL
\\getenv address LIVE_MEMBER_ADDRESS
INSERT INTO personal.member_mail (member_id, email)
SELECT max(id), :'address' FROM personal.member WHERE name = 'TEST LIVE MEMBER';
SQL
}

run_live_briefing() {
  local svc_dir="$TMP/svc" bootstrap="localhost:$KAFKA_PORT" llm_key llm_model
  llm_key=$(live_value ANTHROPIC_API_KEY)
  llm_model=$(env_or_dotenv BRIEFING_LLM_MODEL)
  mkdir -p "$svc_dir/news" "$svc_dir/notifier" "$svc_dir/briefing"

  NEWS_HTTP_PORT=$(free_port)
  echo "news 기동(테스트 전용 데이터베이스 news 스키마, 내부 조회 127.0.0.1:$NEWS_HTTP_PORT)"
  (
    export SPRING_DATASOURCE_URL="$JDBC_URL?currentSchema=news" SPRING_DATASOURCE_USERNAME="$N_USER" SPRING_DATASOURCE_PASSWORD="$N_PW"
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="$bootstrap" SERVER_PORT="$NEWS_HTTP_PORT"
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

  # 받는 사람은 DB 에서 고르므로 테스트 DB 에 멤버·메일·포트폴리오를 한 줄씩 넣는다. 주소는 임시 폴더 파일(권한 600)로만 넘기고 출력하지 않는다.
  echo "테스트 멤버에게 받는 주소 넣기(받는 사람은 DB 에서 고름)"
  seed_live_member || { LIVE_BRIEF_FAIL_REASON="테스트 멤버를 넣지 못함"; return 1; }

  echo "notifier 기동(테스트용: 실제 발송, 제목 앞 [테스트])"
  (
    export SPRING_KAFKA_BOOTSTRAP_SERVERS="$bootstrap"
    export SPRING_DATASOURCE_URL="$JDBC_URL" SPRING_DATASOURCE_USERNAME="$PN_USER" SPRING_DATASOURCE_PASSWORD="$PN_PW"
    # 발송 스위치는 이 프로세스 환경에만 둔다. .env 는 읽지도 쓰지도 않는다(임시 폴더에서 띄움).
    export NOTIFIER_SEND_ENABLED=true NOTIFIER_SUBJECT_PREFIX="[테스트]"
    SMTP_HOST=$(live_value SMTP_HOST) SMTP_USERNAME=$(live_value SMTP_USERNAME)
    SMTP_PASSWORD=$(live_value SMTP_PASSWORD) SMTP_PORT=$(live_value SMTP_PORT)
    export SMTP_HOST SMTP_USERNAME SMTP_PASSWORD
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
    # 근거 자료는 이 실행이 띄운 시세처리기·뉴스처리기 내부 조회 API 로 묻는다.
    export MARKET_API_URL="http://127.0.0.1:$MARKET_HTTP_PORT" NEWS_API_URL="http://127.0.0.1:$NEWS_HTTP_PORT"
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
