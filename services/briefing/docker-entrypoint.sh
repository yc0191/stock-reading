#!/bin/bash
# 브리핑이 읽는 localhost:5432 를 실제 DB 로 넘기는 중계를 띄우고 브리핑을 실행한다.
# 둘 중 하나가 끝나면 나머지도 내린다. 중계만 죽으면 DB 연결이 조용히 끊기므로 컨테이너째 다시 띄우게 한다.
set -eu
: "${DB_HOST:?DB_HOST 가 필요합니다}"
socat "TCP-LISTEN:5432,bind=127.0.0.1,fork,reuseaddr" "TCP:${DB_HOST}:${DB_PORT:-5432}" &
relay=$!
python -m briefing.main &
app=$!
# 종료 신호는 브리핑에 넘겨 정리하고 끝나게 한다.
trap 'kill -TERM "$app" 2>/dev/null || true' TERM INT
status=0
wait -n || status=$?
# 종료 신호로 깨어난 경우 브리핑이 끝나기를 기다린다.
wait "$app" 2>/dev/null || status=$?
kill "$relay" "$app" 2>/dev/null || true
exit "$status"
