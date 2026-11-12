#!/bin/bash
# 서버 스크립트의 실패 경로를 가짜 aws·docker 로 확인한다(외부 호출 없음). 리눅스 서버와 같은 GNU date 가 필요해 ubuntu 컨테이너 안에서 돈다.
#   확인: 다시 띄우기·이미지 받기가 실패하면 지금 버전 기록이 그대로이고 종료 코드가 0이 아니다 / 실패한 긴급 요청은 다음 확인 때 다시 시도한다 /
#         백업은 pg_dump 가 실패하면 S3 에 올리지 않는다 / 비밀 설정의 $·#·작은따옴표·빈 설정·빈 필수 값 / 긴급은 대기 버전과 같을 때만 적용 /
#         잠금을 못 얻으면 실패 / 새 버전을 띄운 뒤 7일 지난 이미지 정리.
# 정상 경로와 금지 시간 경계는 여기서 시험하지 않는다.
# 사용법: infra/deploy/rehearsal/test-server-scripts.sh   (Docker 필요, 종료 코드 0 = 통과)
set -uo pipefail

ROOT=$(git rev-parse --show-toplevel)
IMAGE=${TEST_LINUX_IMAGE:-ubuntu:24.04}
ZONEINFO=${TEST_ZONEINFO:-/usr/share/zoneinfo}
[ -f "$ZONEINFO/Asia/Seoul" ] || { echo "시간대 자료 없음: $ZONEINFO/Asia/Seoul" >&2; exit 2; }

out=$(mktemp)
docker run --rm -i -v "$ZONEINFO":/usr/share/zoneinfo:ro -v "$ROOT":/repo:ro "$IMAGE" bash -s >"$out" 2>&1 <<'IN_CONTAINER'
set -uo pipefail
# 수락 검사는 이 변수 이름이 든 실행 파일을 배포 적용 스크립트로 찾는다. 이 파일이 오인되지 않게 이름의 앞부분을 따로 둔다.
P=DEPLOY_
W=$(mktemp -d); mkdir -p "$W/bin" "$W/param"
cp -r /repo/infra "$W/infra"
APPLY="$W/infra/deploy/server/apply-deploy.sh"; BACKUP="$W/infra/deploy/server/backup-postgres.sh"

# 가짜 aws: Parameter Store 값은 파일에서 읽고, 호출을 기록한다
cat >"$W/bin/aws" <<'FAKE_AWS'
#!/usr/bin/env bash
echo "aws $*" >>"$W/aws.log"
if [ "${1:-}" = ssm ] && [ "${2:-}" = get-parameter ]; then
  f="$W/param/$(printf '%s' "$4" | tr '/' '_')"
  [ -f "$f" ] || { echo "An error occurred (ParameterNotFound)" >&2; exit 254; }
  cat "$f"; echo; exit 0
fi
if [ "${1:-}" = ssm ] && [ "${2:-}" = get-parameters-by-path ]; then
  [ -f "$W/config_out" ] && cat "$W/config_out"
  exit 0
fi
exit 0
FAKE_AWS
# 가짜 docker: FAIL_ON 에 든 하위 명령이 오면 실패한다
cat >"$W/bin/docker" <<'FAKE_DOCKER'
#!/usr/bin/env bash
[ -t 0 ] || cat >/dev/null
echo "docker $*" >>"$W/docker.log"
case " $* " in
  *" ${FAIL_ON:-NEVER} "*) echo "docker 실패 시뮬레이션" >&2; exit 1 ;;
esac
if [ "${1:-} ${2:-}" = "image ls" ] && [ -f "$W/images" ]; then cat "$W/images"; fi
if [ "${1:-}" = exec ]; then
  if [ -n "${DUMP_FAIL:-}" ]; then echo "pg_dump: error" >&2; exit 1; fi
  [ -n "${DUMP_EMPTY:-}" ] || echo "DUMP-DATA"
fi
exit 0
FAKE_DOCKER
chmod +x "$W/bin/aws" "$W/bin/docker"
export W PATH="$W/bin:$PATH"

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "통과  $1"; }
bad() { FAIL=$((FAIL+1)); echo "실패  $1"; tail -5 "$W/out.log" 2>/dev/null | sed 's/^/        /'; }
new_state() { STATE=$(mktemp -d); : >"$W/docker.log"; : >"$W/aws.log"; }
EXTRA=()
run_apply() { # <경로> <시각> [FAIL_ON]   (EXTRA 배열에 든 NAME=값은 환경변수로 더 넣는다)
  env "${EXTRA[@]}" "${P}NOW=$2" "${P}STATE_DIR=$STATE" "${P}ENV_FILE=/nonexistent" FAIL_ON="${3:-}" \
    "${P}STANDBY_PARAM=/t/standby" "${P}URGENT_PARAM=/t/urgent" bash "$APPLY" "$1" >"$W/out.log" 2>&1 </dev/null
  RC=$?
}
current() { cat "$STATE/current" 2>/dev/null || true; }
printf 'V1' >"$W/param/_t_standby"; printf 'U1' >"$W/param/_t_urgent"

echo "== 정기 경로 실패 =="
new_state; run_apply scheduled 2026-11-10T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = V1 ]; } && ok "S1 정상: V1 적용 기록" || bad "S1 정상 적용 기록 (rc=$RC)"
printf 'V2' >"$W/param/_t_standby"
run_apply scheduled 2026-11-11T00:00:05+09:00 up
{ [ "$RC" -ne 0 ] && [ "$(current)" = V1 ]; } && ok "S2 다시 띄우기 실패: 종료 코드 0 아님, 지금 버전 V1 유지" || bad "S2 (rc=$RC, current=$(current))"
: >"$W/docker.log"
run_apply scheduled 2026-11-11T00:00:05+09:00 pull
{ [ "$RC" -ne 0 ] && [ "$(current)" = V1 ] && ! grep -q ' up' "$W/docker.log"; } && ok "S3 이미지 받기 실패: 다시 띄우지 않음, V1 유지" || bad "S3 (rc=$RC, current=$(current))"
run_apply scheduled 2026-11-11T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = V2 ]; } && ok "S4 다음 정기 실행이 V2 를 다시 시도해 적용" || bad "S4 (rc=$RC, current=$(current))"

echo "== 긴급 경로 실패 =="
new_state; printf 'V1' >"$STATE/current"; printf 'U1' >"$W/param/_t_urgent"; printf 'U1' >"$W/param/_t_standby"
run_apply urgent 2026-11-10T20:00:00+09:00 up
{ [ "$RC" -ne 0 ] && [ "$(current)" = V1 ] && [ ! -f "$STATE/urgent_seen" ]; } && ok "U1 다시 띄우기 실패: V1 유지, 본 요청으로 기록하지 않음" || bad "U1 (rc=$RC, current=$(current))"
run_apply urgent 2026-11-10T20:05:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = U1 ]; } && ok "U2 다음 확인에서 같은 요청을 다시 시도해 적용" || bad "U2 (rc=$RC, current=$(current))"

echo "== 긴급은 대기 버전을 앞당길 뿐 =="
new_state; printf 'V1' >"$STATE/current"; printf 'V9' >"$W/param/_t_standby"; printf 'U9' >"$W/param/_t_urgent"
run_apply urgent 2026-11-10T20:00:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = V1 ] && ! grep -q ' up' "$W/docker.log" && grep -q '대기 버전과 다름' "$W/out.log"; } \
  && ok "U3 긴급 U9 != 대기 V9: 적용하지 않고 이유를 로그에 남김" || bad "U3 (rc=$RC, current=$(current))"
[ "$(cat "$STATE/urgent_seen" 2>/dev/null)" = U9 ] && ok "U4 무시한 요청도 본 요청으로 기록" || bad "U4 urgent_seen 이 U9 가 아님"
printf 'U9' >"$W/param/_t_standby"; : >"$W/docker.log"
run_apply urgent 2026-11-10T20:05:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = V1 ] && ! grep -q ' up' "$W/docker.log"; } \
  && ok "U5 나중에 대기 버전이 U9 가 되어도 무시했던 긴급 값으로 낮에 바로 적용하지 않음" || bad "U5 (rc=$RC, current=$(current))"
run_apply scheduled 2026-11-11T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = U9 ]; } && ok "U5b 그 버전은 00:00 정기 배포가 적용" || bad "U5b (rc=$RC, current=$(current))"
printf 'V10' >"$W/param/_t_standby"
run_apply scheduled 2026-11-12T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = V10 ]; } && ok "U6 이후 정기 배포는 대기 버전을 적용(되돌림 없음: 대기 버전이 곧 최신)" || bad "U6 (rc=$RC, current=$(current))"

echo "== 이미지 정리 =="
new_state; printf 'V1' >"$STATE/current"; printf 'V2' >"$W/param/_t_standby"
printf '%s\n' r/stock-reading-market:V0 r/stock-reading-market:V1 r/stock-reading-market:V2 r/stock-reading-migrate:V0 other/image:V0 >"$W/images"
run_apply scheduled 2026-11-10T00:00:05+09:00
{ [ "$RC" -eq 0 ] && grep -q 'image rm r/stock-reading-market:V0' "$W/docker.log" && grep -q 'image rm r/stock-reading-migrate:V0' "$W/docker.log" \
  && ! grep -q 'image rm r/stock-reading-market:V1' "$W/docker.log" && ! grep -q 'image rm r/stock-reading-market:V2' "$W/docker.log" \
  && ! grep -q 'image rm other/image' "$W/docker.log"; } \
  && ok "G1 새 버전(V2)과 바로 전 버전(V1)만 남기고 이 프로젝트의 옛 이미지를 지움" || bad "G1 (rc=$RC)"
rm -f "$W/images"
new_state; run_apply scheduled 2026-11-10T00:00:05+09:00 up
grep -q 'image prune' "$W/docker.log" && bad "G2 다시 띄우기가 실패했는데 이미지를 정리함" || ok "G2 다시 띄우기 실패 때는 정리하지 않음"

echo "== 잠금 =="
new_state; printf 'V1' >"$W/param/_t_standby"
# flock 이 잠금을 얻지 못하는 상황을 가짜 flock 으로 만든다(진짜는 15분을 기다려야 하므로)
printf '#!/bin/sh\nexit 1\n' >"$W/bin/flock"; chmod +x "$W/bin/flock"
run_apply scheduled 2026-11-10T00:00:05+09:00
{ [ "$RC" -eq 1 ] && ! grep -q ' up' "$W/docker.log"; } && ok "L1 정기 경로가 잠금을 못 얻으면 종료 코드 1(실패로 보임)" || bad "L1 (rc=$RC)"
rm -f "$W/bin/flock"

echo "== 비밀 설정(runtime.env) =="
RT() { cat "$STATE/runtime.env" 2>/dev/null; }
good_config() { # 필수 비밀번호 8개 + 특수문자 값
  {
    for n in POSTGRES_ADMIN_PASSWORD MARKET_DB_PASSWORD NEWS_DB_PASSWORD BRIEFING_DB_PASSWORD PERSONAL_OWNER_DB_PASSWORD PERSONAL_WRITER_DB_PASSWORD COLLECTOR_DB_PASSWORD NOTIFIER_DB_PASSWORD; do
      printf '/cfg/%s\tpw-%s\n' "$n" "$n"
    done
    printf '/cfg/SMTP_PASSWORD\ta$b#c d\n'
  } >"$W/config_out"
}
cfg_apply() { EXTRA=("${P}CONFIG_PARAM_PATH=/cfg"); run_apply scheduled 2026-11-10T00:00:05+09:00; EXTRA=(); }
new_state; printf 'V1' >"$W/param/_t_standby"; good_config
cfg_apply
{ [ "$RC" -eq 0 ] && grep -qxF "SMTP_PASSWORD='a\$b#c d'" "$STATE/runtime.env" && grep -qxF "MARKET_DB_PASSWORD='pw-MARKET_DB_PASSWORD'" "$STATE/runtime.env"; } \
  && ok "R1 \$·#·공백이 든 값을 작은따옴표로 감싸 그대로 씀" || bad "R1 (rc=$RC) $(RT | grep SMTP)"
[ "$(stat -c %a "$STATE/runtime.env")" = 600 ] && ok "R2 runtime.env 권한 600" || bad "R2 권한"
new_state; printf 'V1' >"$W/param/_t_standby"; good_config; printf "/cfg/BAD\tit's\n" >>"$W/config_out"
cfg_apply
{ [ "$RC" -ne 0 ] && [ ! -f "$STATE/runtime.env" ] && [ "$(current)" = "" ] && ! grep -q ' up' "$W/docker.log"; } \
  && ok "R3 값에 작은따옴표: 실패로 멈추고 다시 띄우지 않음" || bad "R3 (rc=$RC)"
new_state; printf 'V1' >"$W/param/_t_standby"; : >"$W/config_out"
cfg_apply
{ [ "$RC" -ne 0 ] && [ ! -f "$STATE/runtime.env" ] && ! grep -q ' up' "$W/docker.log" && grep -q '설정이 비어 있음' "$W/out.log"; } \
  && ok "R4 받아 온 설정이 비어 있음: 실패로 멈추고 다시 띄우지 않음" || bad "R4 (rc=$RC)"
new_state; printf 'V1' >"$W/param/_t_standby"; good_config; grep -v 'COLLECTOR_DB_PASSWORD' "$W/config_out" >"$W/config_out.2"; mv "$W/config_out.2" "$W/config_out"
cfg_apply
{ [ "$RC" -ne 0 ] && ! grep -q ' up' "$W/docker.log" && grep -q 'COLLECTOR_DB_PASSWORD' "$W/out.log"; } \
  && ok "R5 필수 비밀번호가 없음: 실패로 멈추고 다시 띄우지 않음" || bad "R5 (rc=$RC)"
new_state; printf 'V1' >"$W/param/_t_standby"; good_config; printf '/cfg/MARKET_DB_PASSWORD\t\n' >>"$W/config_out"
sed -i '/pw-MARKET_DB_PASSWORD/d' "$W/config_out"
cfg_apply
{ [ "$RC" -ne 0 ] && ! grep -q ' up' "$W/docker.log"; } && ok "R6 필수 비밀번호가 빈 값: 실패로 멈춤" || bad "R6 (rc=$RC)"
rm -f "$W/config_out"

echo "== 백업 =="
runb() { BACKUP_S3_BUCKET=test-bucket BACKUP_NOW=2026-11-15T03:30:00+09:00 DUMP_FAIL=${1:-} DUMP_EMPTY=${2:-} bash "$BACKUP" >"$W/out.log" 2>&1 </dev/null; RC=$?; }
: >"$W/aws.log"; runb
{ [ "$RC" -eq 0 ] && grep -q 's3 cp .*s3://test-bucket/postgres/stock-20261115-0330.dump' "$W/aws.log"; } && ok "B1 덤프 성공: 날짜 이름으로 S3 에 올림" || bad "B1 (rc=$RC)"
: >"$W/aws.log"; runb 1
{ [ "$RC" -ne 0 ] && ! grep -q 's3 cp' "$W/aws.log"; } && ok "B2 pg_dump 실패: 올리지 않음" || bad "B2 (rc=$RC)"
: >"$W/aws.log"; runb "" 1
{ [ "$RC" -ne 0 ] && ! grep -q 's3 cp' "$W/aws.log"; } && ok "B3 빈 덤프: 올리지 않음" || bad "B3 (rc=$RC)"

echo "----"; echo "통과 $PASS / 실패 $FAIL"
[ "$FAIL" -eq 0 ] && echo ALL-DONE
IN_CONTAINER
rc=$?
grep -v '^ALL-DONE$' "$out"
# 컨테이너가 중간에 끊겨도 통과로 보이지 않게 끝 표시를 확인한다
if [ "$rc" -eq 0 ] && grep -q '^ALL-DONE$' "$out"; then rm -f "$out"; exit 0; fi
rm -f "$out"; exit 1
