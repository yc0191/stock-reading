#!/usr/bin/env bash
# 데일리 판 시작 알림(daily.collection.completed) 한 건을 Kafka 로 직접 발행한다. 수집기의 알림을 꺼 둔 로컬에서
# 사람이 필요할 때만 데일리 판을 시험하기 위한 도구다. 못 받은 자료는 없음(collected, missingSources 빈 배열)으로 보낸다.
# 브리핑은 같은 날 같은 판을 한 번만 만들므로, 그날 이미 만든 판은 이 알림으로 다시 만들어지지 않는다.
# 수집기 자신의 수집 작업은 scripts/collector-command.sh 를 쓴다(ADR-012). 수집기가 켜져 있고 데일리 판 알림이 켜져 있으면
# collect-post-close 명령만으로도 마감 후 판 알림이 나간다.
#
# 사용법: scripts/publish-daily-notice.sh <morning|post-close>
# 환경변수: KAFKA_CONTAINER (기본 stock-kafka), KAFKA_BOOTSTRAP (컨테이너 안에서 붙을 주소, 기본 localhost:9092)
set -euo pipefail

if [ "$#" -ne 1 ] || { [ "$1" != morning ] && [ "$1" != post-close ]; }; then
  echo "사용법: $0 <morning|post-close>" >&2
  exit 2
fi

edition="$1"
container="${KAFKA_CONTAINER:-stock-kafka}"
# 전체 실행 스크립트나 리허설이 다른 포트로 띄운 Kafka 로 보낼 때 바꾼다.
bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
topic="daily.collection.completed"

# 수집기의 DailyCollectionPublisher 와 같은 형식: 초 단위로 자른 Asia/Seoul 시각, correlationId = daily-<판>-<시각>
now="$(TZ=Asia/Seoul date +%Y-%m-%dT%H:%M:%S+09:00)"
event_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"

json="{\"eventId\":\"${event_id}\",\"eventType\":\"${topic}\",\"schemaVersion\":1,\"occurredAt\":\"${now}\",\"producedAt\":\"${now}\",\"correlationId\":\"daily-${edition}-${now}\",\"payload\":{\"edition\":\"${edition}\",\"status\":\"collected\",\"scheduledFor\":\"${now}\",\"missingSources\":[]}}"

# 메시지 키는 판이다(수집기 발행과 동일). 콘솔 프로듀서는 "키|값" 한 줄을 받는다.
# 콘솔 프로듀서는 보내기에 실패해도(토픽 없음, 시간 초과) 오류만 찍고 0으로 끝나므로 출력의 ERROR 로도 실패를 가린다.
if ! out=$(printf '%s|%s\n' "$edition" "$json" | docker exec -i "$container" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$bootstrap" \
    --topic "$topic" \
    --property parse.key=true \
    --property key.separator='|' 2>&1) || grep -q "ERROR" <<<"$out"; then
  [ -n "$out" ] && echo "$out" >&2
  echo "발행하지 못했습니다: topic=${topic} edition=${edition}" >&2
  exit 1
fi
[ -n "$out" ] && echo "$out" >&2

echo "발행했습니다: topic=${topic} edition=${edition} scheduledFor=${now}"
