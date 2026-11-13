#!/usr/bin/env bash
# schedule.tick 한 건을 Kafka 로 직접 발행한다. 남는 tick(다른 서비스로 가는 신호)을 꺼 둔 로컬에서
# 사람이 필요할 때만 흐름을 시험하기 위한 도구다. 남는 tick 은 둘이다.
#   daily-briefing-morning, daily-briefing-post-close: 브리핑의 데일리 판 시작
# 수집기 자신의 수집 작업(ETF 구성 종목 조회 collect-etf-members 포함)은 schedule.tick 을 받지 않으므로 scripts/collector-command.sh 를 쓴다(ADR-012).
#
# 사용법: scripts/publish-tick.sh <jobType>   (예: daily-briefing-morning, daily-briefing-post-close)
# 환경변수: KAFKA_CONTAINER (기본 stock-kafka), KAFKA_BOOTSTRAP (컨테이너 안에서 붙을 주소, 기본 localhost:9092)
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "사용법: $0 <jobType>  (예: daily-briefing-morning, daily-briefing-post-close)" >&2
  exit 2
fi

job_type="$1"
# JSON 에 그대로 넣으므로 소문자와 하이픈만 허용한다.
if ! [[ "$job_type" =~ ^[a-z][a-z-]*$ ]]; then
  echo "jobType 은 소문자와 하이픈만 쓸 수 있습니다: $job_type" >&2
  exit 2
fi

container="${KAFKA_CONTAINER:-stock-kafka}"
# 전체 실행 스크립트(test-all.sh --live)가 다른 포트로 띄운 일회용 Kafka 로 보낼 때 바꾼다.
bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
topic="schedule.tick"

# 수집기의 DailyBriefingTickPublisher 와 같은 형식: 초 단위로 자른 Asia/Seoul 시각,
# correlationId = jobType-scheduledFor
now="$(TZ=Asia/Seoul date +%Y-%m-%dT%H:%M:%S+09:00)"
event_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"

json="{\"eventId\":\"${event_id}\",\"eventType\":\"schedule.tick\",\"schemaVersion\":1,\"occurredAt\":\"${now}\",\"producedAt\":\"${now}\",\"correlationId\":\"${job_type}-${now}\",\"payload\":{\"jobType\":\"${job_type}\",\"scheduledFor\":\"${now}\"}}"

# 메시지 키는 jobType 이다(수집기 발행과 동일). 콘솔 프로듀서는 "키|값" 한 줄을 받는다.
# 콘솔 프로듀서는 보내기에 실패해도(토픽 없음, 시간 초과) 오류만 찍고 0으로 끝나므로 출력의 ERROR 로도 실패를 가린다.
if ! out=$(printf '%s|%s\n' "$job_type" "$json" | docker exec -i "$container" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$bootstrap" \
    --topic "$topic" \
    --property parse.key=true \
    --property key.separator='|' 2>&1) || grep -q "ERROR" <<<"$out"; then
  [ -n "$out" ] && echo "$out" >&2
  echo "발행하지 못했습니다: topic=${topic} jobType=${job_type}" >&2
  exit 1
fi
[ -n "$out" ] && echo "$out" >&2

echo "발행했습니다: topic=${topic} jobType=${job_type} scheduledFor=${now}"
