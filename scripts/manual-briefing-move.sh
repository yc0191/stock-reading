#!/usr/bin/env bash
# 로컬 Kafka 에 가짜 market.move 한 건과 그 변동에 쓸 수 있는 collection.completed 한 건을 넣고,
# 뉴스처리기가 낸 저장 끝(news.stored)과 브리핑이 낸 briefing.generated 를 읽어 보여 준다. 사람이 흐름을 직접 확인하기 위한 도구다.
# 브리핑은 시세·기사를 시세처리기·뉴스처리기 내부 조회 API 로 직접 부르므로 그 호출은 Kafka 가 아니라 작업 기록(briefing.job_step)에 남는다.
#
# 미리 띄워 둘 것: 로컬 인프라(Kafka, Postgres), 브리핑(python -m briefing.main), 시세처리기(market), 뉴스(news).
# 뉴스처리기가 꺼져 있으면 저장 끝이 오지 않아 근거 조회가 시작되지 않고, 시세처리기가 꺼져 있으면 호출이 재시도 끝에 실패로 기록된다.
#
# 사용법: scripts/manual-briefing-move.sh [symbol]   (기본 TEST01, 가짜 종목)
# 환경변수: KAFKA_CONTAINER (기본 stock-kafka), WAIT_SECONDS (결과를 기다리는 시간, 기본 60)
set -euo pipefail

symbol="${1:-TEST01}"
# JSON 에 그대로 넣으므로 영문 대문자와 숫자만 허용한다.
if ! [[ "$symbol" =~ ^[A-Z0-9]+$ ]]; then
  echo "symbol 은 영문 대문자와 숫자만 쓸 수 있습니다: $symbol" >&2
  exit 2
fi

container="${KAFKA_CONTAINER:-stock-kafka}"
wait_seconds="${WAIT_SECONDS:-60}"

# 쓸 수 있는 완료는 startedAt >= 변동 시각 + 5분이어야 한다. 그래서 변동 시각을 6분 전으로 두고 수집은 지금 시작한 것으로 한다.
read -r move_at started_at finished_at < <(python3 -c '
from datetime import datetime, timedelta, timezone
now = datetime.now(timezone(timedelta(hours=9))).replace(microsecond=0)
print((now - timedelta(minutes=6)).isoformat(), now.isoformat(), (now + timedelta(seconds=1)).isoformat())
')
move_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
completed_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
correlation_id="manual-${move_at}"

move="{\"eventId\":\"${move_id}\",\"eventType\":\"market.move\",\"schemaVersion\":1,\"occurredAt\":\"${move_at}\",\"producedAt\":\"${move_at}\",\"correlationId\":\"${correlation_id}\",\"payload\":{\"symbol\":\"${symbol}\",\"previousPrice\":10000,\"price\":9850,\"changePercent\":-1.5,\"thresholdPercent\":1.0,\"asOf\":\"${move_at}\",\"detectionType\":\"window\",\"moveStartedAt\":\"${move_at}\"}}"
completed="{\"eventId\":\"${completed_id}\",\"eventType\":\"collection.completed\",\"schemaVersion\":1,\"occurredAt\":\"${finished_at}\",\"producedAt\":\"${finished_at}\",\"correlationId\":\"collect-news-${started_at}\",\"payload\":{\"jobType\":\"collect-news\",\"scheduledFor\":\"${started_at}\",\"startedAt\":\"${started_at}\",\"finishedAt\":\"${finished_at}\",\"publishedCount\":0,\"articleEventIds\":[],\"incompleteSymbols\":[]}}"

produce() {  # 콘솔 프로듀서는 "키|값" 한 줄을 받는다.
  printf '%s|%s\n' "$2" "$3" | docker exec -i "$container" \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$1" \
    --property parse.key=true --property key.separator='|'
}

# 키는 market.move 가 종목코드, collection.completed 가 jobType 이다(생산하는 서비스와 같다).
produce market.move "$symbol" "$move"
produce collection.completed collect-news "$completed"
echo "발행했습니다: market.move eventId=${move_id} symbol=${symbol} asOf=${move_at}"
echo "발행했습니다: collection.completed startedAt=${started_at}"
echo "${wait_seconds}초 동안 결과를 기다립니다..."
sleep "$wait_seconds"

show() {  # 처음부터 읽고 이번 변동의 jobId·correlationId 또는 이번 수집 완료의 eventId 가 든 메시지만 보인다.
  echo "--- $1"
  docker exec "$container" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$1" --from-beginning --timeout-ms 5000 --property print.key=true 2>/dev/null \
    | grep -F -e "$move_id" -e "$correlation_id" -e "$completed_id" || echo "(없음)"
}

show news.stored
show briefing.generated
echo "시세·기사 조회와 LLM 호출 기록은 briefing.job_step 에, briefing.generated 가 없으면 그 이유는 작업 기록(briefing.job_run·briefing.job_log)에 있습니다: python3 scripts/job_history.py ${move_id}"
