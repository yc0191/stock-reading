#!/usr/bin/env bash
# 수집기에 수동 실행 명령(collector.command) 한 건을 Kafka 로 보낸다. 수집기는 받은 즉시 그 수집 작업을 1회
# 실행하며, 시세 시간대와 타이머 켜기 설정(COLLECTOR_SCHEDULE_ENABLED 등)을 보지 않는다(ADR-012).
# 같은 작업이 돌고 있으면 건너뛰고 수집기 로그에만 남는다. 수집기가 꺼져 있을 때 보낸 명령은 켜진 뒤 실행되지 않는다.
#
# 사용법: scripts/collector-command.sh <명령>
#   명령: collect | collect-news | collect-pre-open | collect-intraday | collect-post-close | collect-etf-members
#   (예: collect 는 1초 현재가 한 회차, collect-news 는 뉴스 수집 한 번)
# 환경변수: KAFKA_CONTAINER (기본 stock-kafka), KAFKA_BOOTSTRAP (컨테이너 안에서 붙을 주소, 기본 localhost:9092)
set -euo pipefail

usage="사용법: $0 <collect|collect-news|collect-pre-open|collect-intraday|collect-post-close|collect-etf-members>"

if [ "$#" -ne 1 ]; then
  echo "$usage" >&2
  exit 2
fi

command="$1"
# 수집기는 모르는 명령을 무시만 하므로, 오타를 보낸 줄 모르고 기다리지 않게 여기서 막는다.
case "$command" in
  collect|collect-news|collect-pre-open|collect-intraday|collect-post-close|collect-etf-members) ;;
  *)
    echo "모르는 명령입니다: $command" >&2
    echo "$usage" >&2
    exit 2
    ;;
esac

container="${KAFKA_CONTAINER:-stock-kafka}"
# 전체 실행 스크립트(test-all.sh --live)가 다른 포트로 띄운 일회용 Kafka 로 보낼 때 바꾼다.
bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
topic="collector.command"

now="$(TZ=Asia/Seoul date +%Y-%m-%dT%H:%M:%S+09:00)"
event_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"

json="{\"eventId\":\"${event_id}\",\"eventType\":\"collector.command\",\"schemaVersion\":1,\"occurredAt\":\"${now}\",\"producedAt\":\"${now}\",\"correlationId\":\"command-${command}-${now}\",\"payload\":{\"command\":\"${command}\"}}"

# 메시지 키는 명령 이름이다. 콘솔 프로듀서는 "키|값" 한 줄을 받는다.
# 콘솔 프로듀서는 보내기에 실패해도(토픽 없음, 시간 초과) 오류만 찍고 0으로 끝나므로 출력의 ERROR 로도 실패를 가린다.
if ! out=$(printf '%s|%s\n' "$command" "$json" | docker exec -i "$container" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$bootstrap" \
    --topic "$topic" \
    --property parse.key=true \
    --property key.separator='|' 2>&1) || grep -q "ERROR" <<<"$out"; then
  [ -n "$out" ] && echo "$out" >&2
  echo "보내지 못했습니다: topic=${topic} command=${command}" >&2
  exit 1
fi
[ -n "$out" ] && echo "$out" >&2

echo "보냈습니다: topic=${topic} command=${command} eventId=${event_id}"
