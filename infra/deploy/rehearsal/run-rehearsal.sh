#!/bin/bash
# 로컬 리허설: 서버 2대(각 메모리 상한 2GB)를 Docker 컨테이너(dind)로 흉내 내 운영 Compose 전체를 띄우고,
# 가짜 값을 넣고 가짜 tick 으로 한 바퀴 돌린 뒤 서비스별 메모리를 잰다. 외부 API·AWS·메일은 부르지 않는다(시세는 시뮬레이터, 메일은 드라이런, LLM 키 없음).
#
#   데이터 서버  rehearsal-data      172.30.0.10  data.yml     (Kafka, Postgres 는 이 주소에만 포트를 연다)
#   서비스 서버  rehearsal-services  172.30.0.11  services.yml (서비스 5개와 DB 표 만들기 단계)
#
# 사용법: infra/deploy/rehearsal/run-rehearsal.sh [all|up|seed|tick|measure|down]   (기본 all)
#   all      build -> up -> seed -> tick -> 기다림 -> 두 번째 배포 -> 측정 요약 -> down   (메모리는 시작부터 끝까지 5초마다 잰다)
#   up       서버 2대를 만들고 이미지를 옮겨 데이터 서버, 서비스 서버 순서로 띄운다
#   seed     가짜 종목·멤버·포트폴리오를 DB 에 넣고 수집기·뉴스를 다시 띄운다(두 서비스는 시작할 때 DB 를 읽는다)
#   tick     수집기 수동 명령과 데일리 판 tick 을 Kafka 로 넣는다
#   redeploy 서비스가 도는 중에 새 버전 태그(dev2)로 두 번째 배포를 하고, 그 동안 서비스 서버 전체 메모리를 1초 안팎 간격으로 잰다
#   measure  지금까지 잰 값의 최댓값을 표로 보인다
#   down     서버 2대와 네트워크, 임시 파일을 지운다
# 환경변수: REHEARSAL_MEM(서버 메모리 상한, 기본 2g), REHEARSAL_WAIT(tick 뒤 기다리는 초, 기본 150), REHEARSAL_KEEP=1(끝나도 지우지 않음).
# Docker 에 메모리를 4.5GB 이상 줘야 한다. 개발용 stock-* 컨테이너와 볼륨은 건드리지 않는다(이 스크립트가 만드는 이름은 rehearsal-* 뿐).
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
NET=rehearsal-net
SUBNET=172.30.0.0/24
DATA=rehearsal-data
DATA_IP=172.30.0.10
SVC=rehearsal-services
SVC_IP=172.30.0.11
MEM=${REHEARSAL_MEM:-2g}
WAIT=${REHEARSAL_WAIT:-150}
DIND_IMAGE=docker:27-dind
OUT=${REHEARSAL_OUT:-$ROOT/build/rehearsal}
SAMPLES=$OUT/memory-samples.csv
ENV_DIR=$OUT/env
DATA_COMPOSE="docker compose -f /repo/infra/deploy/compose/data.yml --env-file /etc/rehearsal/data.env"
SVC_COMPOSE="docker compose -f /repo/infra/deploy/compose/services.yml --env-file /etc/rehearsal/services.env"
SERVICE_IMAGES="collector market news notifier briefing migrate"

mkdir -p "$OUT"

in_data() { docker exec "$DATA" "$@"; }
in_svc() { docker exec "$SVC" "$@"; }

start_server() { # <이름> <사설 주소>
  docker rm -f "$1" >/dev/null 2>&1 || true
  # --memory-swap 를 같게 줘서 상한을 넘으면 스왑으로 넘기지 않고 바로 강제 종료되게 한다(작은 서버처럼).
  docker run -d --name "$1" --privileged --network "$NET" --ip "$2" --memory "$MEM" --memory-swap "$MEM" \
    -e DOCKER_TLS_CERTDIR= -v "$ROOT":/repo:ro "$DIND_IMAGE" >/dev/null
  for _ in $(seq 1 60); do
    docker exec "$1" docker info >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "서버 $1 의 Docker 가 시작되지 않음" >&2
  return 1
}

# 이번 실행에서만 쓰는 임의 비밀번호로 설정 파일을 만든다(저장소 밖 build/ 아래, 권한 600, 끝나면 지움).
make_env_files() {
  mkdir -p "$ENV_DIR"
  chmod 700 "$ENV_DIR"
  pw() { openssl rand -hex 12; }
  (
    umask 077
    a=$(pw); m=$(pw); n=$(pw); b=$(pw); po=$(pw); pwr=$(pw); c=$(pw); no=$(pw)
    {
      echo "DATA_PRIVATE_IP=$DATA_IP"
      echo "POSTGRES_DB=stock"
      echo "POSTGRES_ADMIN_USER=postgres"
      echo "POSTGRES_ADMIN_PASSWORD=$a"
      echo "MARKET_DB_USER=market_svc"
      echo "MARKET_DB_PASSWORD=$m"
      echo "NEWS_DB_USER=news_svc"
      echo "NEWS_DB_PASSWORD=$n"
      echo "BRIEFING_DB_USER=briefing_svc"
      echo "BRIEFING_DB_PASSWORD=$b"
    } >"$ENV_DIR/data.env"
    {
      cat "$ENV_DIR/data.env"
      echo "PERSONAL_OWNER_DB_USER=personal_owner"
      echo "PERSONAL_OWNER_DB_PASSWORD=$po"
      echo "PERSONAL_WRITER_DB_USER=personal_writer"
      echo "PERSONAL_WRITER_DB_PASSWORD=$pwr"
      echo "COLLECTOR_DB_USER=collector_svc"
      echo "COLLECTOR_DB_PASSWORD=$c"
      echo "NOTIFIER_DB_USER=notifier_svc"
      echo "NOTIFIER_DB_PASSWORD=$no"
      # 타이머는 모두 꺼 두고(기본값), 시세는 시뮬레이터, 메일은 드라이런, LLM 키는 비움 -> 규칙 기반 리포트
      echo "COLLECTOR_MARKET_DATA_SOURCE=simulator"
      echo "NOTIFIER_SEND_ENABLED=false"
    } >"$ENV_DIR/services.env"
  )
}

stop_sampler() {
  if [ -f "$OUT/sampler.pid" ]; then
    kill "$(cat "$OUT/sampler.pid")" 2>/dev/null || true
    wait "$(cat "$OUT/sampler.pid")" 2>/dev/null || true
    rm -f "$OUT/sampler.pid"
  fi
}

# 5초마다 컨테이너별 메모리와 서버(컨테이너 전체, Docker 데몬 포함) 메모리를 CSV 로 쌓는다.
start_sampler() {
  stop_sampler
  echo "time,server,container,mem_used" >"$SAMPLES"
  (
    while true; do
      now=$(date +%H:%M:%S)
      docker stats --no-stream --format "$now,host,{{.Name}},{{.MemUsage}}" "$DATA" "$SVC" 2>/dev/null | sed 's#,host,rehearsal-data,#,data-server,total,#; s#,host,rehearsal-services,#,services-server,total,#' >>"$SAMPLES" || true
      for pair in "$DATA:data-server" "$SVC:services-server"; do
        docker exec "${pair%%:*}" docker stats --no-stream --format "$now,${pair##*:},{{.Name}},{{.MemUsage}}" 2>/dev/null >>"$SAMPLES" || true
      done
      sleep 5
    done
  ) >/dev/null 2>&1 &
  echo $! >"$OUT/sampler.pid"
}

cmd_build() {
  echo "== 서비스 이미지 만들기 =="
  # 필수 비밀번호가 비면 Compose 가 어떤 명령이든 실패하므로, 빌드에는 아무 값이나 채운 임시 설정을 준다(이미지에는 들어가지 않음).
  local dummy
  dummy=$(mktemp)
  for name in POSTGRES_ADMIN NEWS MARKET BRIEFING PERSONAL_OWNER PERSONAL_WRITER COLLECTOR NOTIFIER; do
    case $name in POSTGRES_ADMIN) echo "POSTGRES_ADMIN_PASSWORD=x" ;; *) echo "${name}_DB_PASSWORD=x" ;; esac
  done >"$dummy"
  docker compose -f "$ROOT/infra/deploy/compose/services.yml" --env-file "$dummy" build
  rm -f "$dummy"
}

cmd_up() {
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet "$SUBNET" "$NET" >/dev/null
  make_env_files
  echo "== 서버 2대 만들기(메모리 상한 $MEM) =="
  start_server "$DATA" "$DATA_IP"
  start_server "$SVC" "$SVC_IP"
  start_sampler

  echo "== 이미지 옮기기 =="
  docker save apache/kafka:4.2.2 pgvector/pgvector:0.8.7-pg17-trixie | docker exec -i "$DATA" docker load >/dev/null
  local images=""
  for name in $SERVICE_IMAGES; do images="$images local/stock-reading-$name:dev"; done
  # shellcheck disable=SC2086
  docker save $images | docker exec -i "$SVC" docker load >/dev/null
  in_data mkdir -p /etc/rehearsal
  in_svc mkdir -p /etc/rehearsal
  docker cp "$ENV_DIR/data.env" "$DATA:/etc/rehearsal/data.env"
  docker cp "$ENV_DIR/services.env" "$SVC:/etc/rehearsal/services.env"

  echo "== 데이터 서버 띄우기(Kafka, Postgres) =="
  in_data sh -c "$DATA_COMPOSE up -d --wait"
  echo "== 사설 주소에서만 열렸는지 확인 =="
  in_svc sh -c "nc -z -w 3 $DATA_IP 9092 && nc -z -w 3 $DATA_IP 5432" && echo "서비스 서버 -> 데이터 서버 사설 주소: Kafka 9092, Postgres 5432 닿음"
  if in_data sh -c "nc -z -w 2 127.0.0.1 9092 || nc -z -w 2 127.0.0.1 5432" >/dev/null 2>&1; then
    echo "실패: 데이터 서버의 127.0.0.1 에도 포트가 열려 있음" >&2
    return 1
  fi
  echo "데이터 서버 127.0.0.1: 닫힘(포트는 사설 주소에만 바인딩)"

  echo "== 서비스 서버 띄우기(DB 표 만들기 -> 서비스 5개) =="
  in_svc sh -c "DEPLOY_VERSION=dev $SVC_COMPOSE up -d"
  in_svc sh -c "DEPLOY_VERSION=dev $SVC_COMPOSE ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}'" | tee "$OUT/services-state.txt"
}

cmd_seed() {
  echo "== 가짜 종목·멤버·포트폴리오 넣기 =="
  {
    echo "SET search_path TO market;"
    cat "$ROOT/docs/examples/holdings.example.sql"
    # 서로 다른 포트폴리오의 두 번째 멤버(멤버마다 맞춤 메일 확인용)
    cat <<'SQL'
INSERT INTO personal.member (name) VALUES ('TEST MEMBER TWO');
INSERT INTO personal.member_mail (member_id, email)
SELECT id, 'member2@example.com' FROM personal.member WHERE name = 'TEST MEMBER TWO';
INSERT INTO personal.member_portfolio (member_id, etf_id, relation)
SELECT m.id, e.id, 'HELD' FROM personal.member m, market.etf e WHERE m.name = 'TEST MEMBER TWO' AND e.symbol = 'TEST02';
SQL
  } | docker exec -i "$DATA" docker exec -i stock-postgres psql -q -U postgres -d stock -v ON_ERROR_STOP=1 >/dev/null
  echo "수집기·뉴스 다시 띄우기(시작할 때 DB 를 읽음)"
  in_svc sh -c "DEPLOY_VERSION=dev $SVC_COMPOSE restart collector news" >/dev/null
  sleep 20
}

# 데이터 서버의 Kafka 컨테이너에 명령을 넣는 repo 스크립트를 쓰려고, docker 를 데이터 서버 안의 docker 로 넘기는 얇은 껍데기를 PATH 앞에 둔다.
with_data_docker() {
  local shim="$OUT/shim"
  mkdir -p "$shim"
  # 껍데기 안의 docker 는 이 껍데기가 아니라 진짜 docker 여야 하므로 절대 경로로 부른다.
  printf '#!/bin/sh\nexec %s exec -i %s docker "$@"\n' "$(command -v docker)" "$DATA" >"$shim/docker"
  chmod +x "$shim/docker"
  PATH="$shim:$PATH" KAFKA_BOOTSTRAP=localhost:19092 "$@"
}

cmd_tick() {
  echo "== 가짜 tick 한 바퀴 =="
  local c
  for c in collect collect-pre-open collect-intraday collect-post-close collect-news; do
    with_data_docker "$ROOT/scripts/collector-command.sh" "$c"
    sleep 3
  done
  for c in collect-members daily-briefing-morning daily-briefing-post-close; do
    with_data_docker "$ROOT/scripts/publish-tick.sh" "$c"
  done
}

cmd_wait_and_check() {
  echo "== $WAIT 초 기다린 뒤 확인 =="
  sleep "$WAIT"
  : >"$OUT/checks.txt"
  {
    echo "[서비스 상태]"
    in_svc sh -c "DEPLOY_VERSION=dev $SVC_COMPOSE ps -a --format '{{.Service}} {{.State}} exit={{.ExitCode}}'"
    echo "[서비스 재시작 횟수(0 이어야 함)]"
    for name in collector market news notifier briefing; do
      echo "$name $(in_svc docker inspect -f '{{.RestartCount}} oom={{.State.OOMKilled}}' "stock-services-$name-1")"
    done
    echo "[브리핑 결과 토픽 briefing.daily.generated 의 메시지 수(파티션:끝 오프셋)]"
    in_data docker exec stock-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic briefing.daily.generated
    echo "[발송기 드라이런 줄 수]"
    in_svc docker logs stock-services-notifier-1 2>&1 | grep -ci 'dry' || true
    echo "[서비스별 ERROR 로그 줄 수]"
    for name in collector market news notifier briefing; do
      echo "$name $(in_svc docker logs "stock-services-$name-1" 2>&1 | grep -c ' ERROR ' || true)"
    done
  } | tee "$OUT/checks.txt"
}

# 운영 배포와 같은 상황: 서비스 5개가 도는 중에 새 버전 태그로 `up -d` 를 하면 DB 표 만들기 단계가 다시 돌고 서비스가 새로 만들어진다.
# 이 순간 옛 서비스와 새 서비스, 마이그레이션 컨테이너가 잠깐 겹쳐 서버 메모리가 가장 크다.
cmd_redeploy() {
  echo "== 서비스가 도는 중 두 번째 배포(새 버전 태그 dev2) =="
  local name file="$OUT/redeploy-samples.csv" pid
  for name in $SERVICE_IMAGES; do
    in_svc docker tag "local/stock-reading-$name:dev" "local/stock-reading-$name:dev2"
  done
  echo "time,container,mem_used" >"$file"
  (
    while true; do
      now=$(date +%H:%M:%S)
      docker stats --no-stream --format "$now,services-server-total,{{.MemUsage}}" "$SVC" 2>/dev/null >>"$file" || true
      docker exec "$SVC" docker stats --no-stream --format "$now,{{.Name}},{{.MemUsage}}" 2>/dev/null >>"$file" || true
    done
  ) >/dev/null 2>&1 &
  pid=$!
  in_svc sh -c "DEPLOY_VERSION=dev2 $SVC_COMPOSE up -d"
  sleep 60
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  {
    echo "[두 번째 배포 뒤 서비스 상태]"
    in_svc sh -c "DEPLOY_VERSION=dev2 $SVC_COMPOSE ps -a --format '{{.Service}} {{.State}} exit={{.ExitCode}}'"
    echo "[재시작 횟수와 OOM 종료(둘 다 0·false 여야 함)]"
    for name in collector market news notifier briefing; do
      echo "$name $(in_svc docker inspect -f '{{.RestartCount}} oom={{.State.OOMKilled}}' "stock-services-$name-1")"
    done
    echo "[DB 표 만들기 단계: 실행 사용자(root 가 아니어야 함)와 종료 코드]"
    in_svc docker inspect -f 'user={{.Config.User}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' stock-services-flyway-migrate-1
    echo "[두 번째 배포 동안 서비스 서버 전체 메모리]"
    python3 - "$file" <<'PY'
import csv, sys
UNIT = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}
def mib(text):
    used = text.split("/")[0].strip()
    for unit in ("GiB", "MiB", "KiB", "B"):
        if used.endswith(unit):
            return float(used[: -len(unit)]) * UNIT[unit]
    raise ValueError(text)
total, peak, n = 0.0, {}, 0
with open(sys.argv[1]) as f:
    for row in csv.DictReader(f):
        try:
            v = mib(row["mem_used"])
        except (ValueError, KeyError, TypeError):
            continue
        if row["container"] == "services-server-total":
            total = max(total, v); n += 1
        else:
            peak[row["container"]] = max(peak.get(row["container"], 0), v)
print(f"서비스 서버 전체(Docker 데몬 포함) 최대 {total:.0f} MiB (잰 횟수 {n}, 상한 2048)")
for k in sorted(peak):
    print(f"  {k} 최대 {peak[k]:.0f} MiB")
PY
  } | tee "$OUT/redeploy-summary.txt"
}

cmd_measure() {
  [ -f "$SAMPLES" ] || { echo "잰 값이 없음: $SAMPLES" >&2; return 1; }
  python3 - "$SAMPLES" <<'PY'
import csv, sys
UNIT = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}

def mib(text):
    used = text.split("/")[0].strip()
    for unit in ("GiB", "MiB", "KiB", "B"):
        if used.endswith(unit):
            return float(used[: -len(unit)]) * UNIT[unit]
    raise ValueError(text)

peak, last = {}, {}
with open(sys.argv[1]) as f:
    for row in csv.DictReader(f):
        try:
            value = mib(row["mem_used"])
        except (ValueError, KeyError):
            continue
        if row["container"] == "--":
            continue
        key = (row["server"], row["container"])
        peak[key] = max(peak.get(key, 0), value)
        last[key] = value
print(f"{'서버':16} {'컨테이너':32} {'최대(MiB)':>10} {'마지막(MiB)':>12}")
for key in sorted(peak):
    print(f"{key[0]:16} {key[1]:32} {peak[key]:10.0f} {last[key]:12.0f}")
for server in ("data-server", "services-server"):
    inner = sum(v for (s, c), v in peak.items() if s == server and c != "total")
    total = peak.get((server, "total"), 0)
    print(f"{server}: 컨테이너 최대 합 {inner:.0f} MiB, 서버 전체(Docker 데몬 포함) 최대 {total:.0f} MiB")
PY
}

cmd_down() {
  stop_sampler
  docker rm -f "$DATA" "$SVC" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  rm -rf "$ENV_DIR" "$OUT/shim"
  echo "리허설 서버와 임시 파일을 지웠습니다(측정 기록은 $OUT 에 남음)"
}

case "${1:-all}" in
  build) cmd_build ;;
  up) cmd_up ;;
  seed) cmd_seed ;;
  tick) cmd_tick ;;
  redeploy) cmd_redeploy ;;
  measure) cmd_measure ;;
  down) cmd_down ;;
  all)
    if [ "${REHEARSAL_KEEP:-0}" != 1 ]; then trap cmd_down EXIT; fi
    cmd_build
    cmd_up
    cmd_seed
    cmd_tick
    cmd_wait_and_check
    cmd_redeploy
    stop_sampler
    cmd_measure | tee "$OUT/memory-summary.txt"
    ;;
  *) echo "사용법: $0 [all|build|up|seed|tick|redeploy|measure|down]" >&2; exit 64 ;;
esac
