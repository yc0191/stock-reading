#!/bin/bash
# 서버 스크립트의 실패 경로를 가짜 aws·docker 와 진짜 git 으로 확인한다(외부 호출 없음). 리눅스 서버와 같은 GNU date 와 git 이 필요해
# 우분투 기반 컨테이너 안에서 돈다. 서버의 저장소 사본은 컨테이너 안 가짜 원격 저장소의 클론이고, 버전은 그 저장소의 커밋 해시다.
#   확인: 다시 띄우기·이미지 받기·DB 표 만들기가 실패하면 지금 버전 기록이 그대로이고 종료 코드가 0이 아니다 / 표 만들기가 실패하면 서비스를 바꾸지 않는다 /
#         표 만들기는 서비스와 따로 한 번만 돈다 / 사본을 버전 커밋으로 맞추고 잠금을 넘겨받아 새 스크립트로 다시 실행한다 / 사본에 고치거나 새로 만든 파일,
#         기본 가지에 없는 커밋, fetch 실패, 해시가 아닌 버전, 다시 실행 뒤 대상이 바뀜이면 사본도 서비스도 건드리지 않는다 / 복구 경로는 금지 시간에도 대기 버전을 적용한다 /
#         실패한 긴급 요청은 다음 확인 때 다시 시도한다 / 백업은 pg_dump 가 실패하면 S3 에 올리지 않는다 / 비밀 설정의 $·#·작은따옴표·빈 설정·빈 필수 값 /
#         긴급은 대기 버전과 같을 때만 적용 / 잠금을 못 얻으면 실패 / 새 버전과 바로 전 버전만 남기는 이미지 정리.
# 정기·긴급 경로의 금지 시간 경계는 여기서 시험하지 않는다.
# 사용법: infra/deploy/rehearsal/test-server-scripts.sh   (Docker 필요, 종료 코드 0 = 통과)
set -uo pipefail

ROOT=$(git rev-parse --show-toplevel)
# 우분투 24.04 에 git 을 미리 넣은 공식 이미지(서버와 같은 우분투)
IMAGE=${TEST_LINUX_IMAGE:-buildpack-deps:noble-scm}
ZONEINFO=${TEST_ZONEINFO:-/usr/share/zoneinfo}
[ -f "$ZONEINFO/Asia/Seoul" ] || { echo "시간대 자료 없음: $ZONEINFO/Asia/Seoul" >&2; exit 2; }

out=$(mktemp)
docker run --rm -i -v "$ZONEINFO":/usr/share/zoneinfo:ro -v "$ROOT":/repo:ro "$IMAGE" bash -s >"$out" 2>&1 <<'IN_CONTAINER'
set -uo pipefail
# 수락 검사는 이 변수 이름이 든 실행 파일을 배포 적용 스크립트로 찾는다. 이 파일이 오인되지 않게 이름의 앞부분을 따로 둔다.
P=DEPLOY_
W=$(mktemp -d); mkdir -p "$W/bin" "$W/param"
APPLY="$W/repo/infra/deploy/server/apply-deploy.sh"; BACKUP="$W/repo/infra/deploy/server/backup-postgres.sh"

# 가짜 원격 저장소와 서버의 저장소 사본. 버전마다 커밋 하나이고, 마지막 V2 만 스크립트를 바꿔 다시 실행됐는지 보이게 한다.
git config --global user.email test@example.invalid; git config --global user.name test
git config --global init.defaultBranch main; git config --global advice.detachedHead false
git init -q --bare "$W/origin.git"
git clone -q "$W/origin.git" "$W/src" 2>/dev/null; cp -r /repo/infra "$W/src/infra"
git -C "$W/src" add -A; git -C "$W/src" commit -qm V1; V1=$(git -C "$W/src" rev-parse HEAD)
for v in U1 U9 V10; do git -C "$W/src" commit -q --allow-empty -m "$v"; printf -v "$v" '%s' "$(git -C "$W/src" rev-parse HEAD)"; done
sed -i '0,/^set -euo pipefail$/s//set -euo pipefail\necho NEW-SCRIPT-RAN/' "$W/src/infra/deploy/server/apply-deploy.sh"
git -C "$W/src" commit -qam V2; V2=$(git -C "$W/src" rev-parse HEAD)
git -C "$W/src" push -q -u origin HEAD
# 기본 가지가 아닌 가지에만 있는 커밋(포크 커밋을 흉내 냄)
git -C "$W/src" checkout -q -b side; git -C "$W/src" commit -q --allow-empty -m side; SIDE=$(git -C "$W/src" rev-parse HEAD)
git -C "$W/src" push -q origin side
git clone -q "$W/origin.git" "$W/repo"; git -C "$W/repo" checkout -q --detach "$V1"
# 사본을 만든 뒤 원격에만 올린 커밋(운영처럼 fetch 해야만 사본에 들어온다)
git -C "$W/src" checkout -q main; git -C "$W/src" commit -q --allow-empty -m V3; V3=$(git -C "$W/src" rev-parse HEAD)
git -C "$W/src" push -q origin HEAD
head_of() { git -C "$W/repo" rev-parse HEAD; }

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
# 가짜 docker: FAIL_ON 에 든 하위 명령이 오면 실패한다. 서비스 목록은 사본의 Compose 파일에서 실제로 읽는다.
cat >"$W/bin/docker" <<'FAKE_DOCKER'
#!/usr/bin/env bash
[ -t 0 ] || cat >/dev/null
echo "docker $*" >>"$W/docker.log"
case " $* " in
  *" ${FAIL_ON:-NEVER} "*) echo "docker 실패 시뮬레이션" >&2; exit 1 ;;
  *" config --services "*) sed -n 's/^  \([a-z][a-z-]*\):$/\1/p' "$W/repo/infra/deploy/compose/services.yml"; exit 0 ;;
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
setp() { printf '%s' "$2" >"$W/param/_t_$1"; }
reset_v1() { git -C "$W/repo" checkout -q --detach "$V1"; new_state; printf '%s' "$V1" >"$STATE/current"; }
setp standby "$V1"; setp urgent "$U1"

echo "== 정기 경로 실패 =="
new_state; run_apply scheduled 2026-11-10T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V1" ]; } && ok "S1 정상: V1 적용 기록" || bad "S1 정상 적용 기록 (rc=$RC)"
setp standby "$V2"
run_apply scheduled 2026-11-11T00:00:05+09:00 up
{ [ "$RC" -ne 0 ] && [ "$(current)" = "$V1" ]; } && ok "S2 다시 띄우기 실패: 종료 코드 0 아님, 지금 버전 V1 유지" || bad "S2 (rc=$RC, current=$(current))"
: >"$W/docker.log"
run_apply scheduled 2026-11-11T00:00:05+09:00 pull
{ [ "$RC" -ne 0 ] && [ "$(current)" = "$V1" ] && ! grep -q ' up' "$W/docker.log"; } && ok "S3 이미지 받기 실패: 다시 띄우지 않음, V1 유지" || bad "S3 (rc=$RC, current=$(current))"
run_apply scheduled 2026-11-11T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V2" ]; } && ok "S4 다음 정기 실행이 V2 를 다시 시도해 적용" || bad "S4 (rc=$RC, current=$(current))"

echo "== DB 표 만들기를 먼저 따로 =="
reset_v1; setp standby "$V2"
run_apply scheduled 2026-11-11T00:00:05+09:00 run
{ [ "$RC" -ne 0 ] && [ "$(current)" = "$V1" ] && ! grep -q ' up' "$W/docker.log" && grep -q 'DB 표 만들기' "$W/out.log"; } \
  && ok "M1 표 만들기 실패: 서비스를 바꾸지 않음(up 없음), V1 유지, 종료 코드 0 아님" || bad "M1 (rc=$RC, current=$(current))"
: >"$W/docker.log"
run_apply scheduled 2026-11-11T00:00:05+09:00
upline=$(grep ' up ' "$W/docker.log")
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V2" ] && [ "$(grep -c 'run --rm -T --no-deps flyway-migrate' "$W/docker.log")" -eq 1 ] \
  && [[ "$upline" == *"up -d --no-deps --remove-orphans collector market news notifier briefing ops-checker" ]] && [[ "$upline" != *flyway-migrate* ]] \
  && [ "$(grep -n 'flyway-migrate' "$W/docker.log" | head -1 | cut -d: -f1)" -lt "$(grep -n ' up ' "$W/docker.log" | cut -d: -f1)" ]; } \
  && ok "M2 표 만들기를 먼저 한 번만 돌리고, up 은 표 만들기를 뺀 서비스만 --no-deps 로 띄움" || bad "M2 (rc=$RC) $upline"

echo "== 저장소 사본 맞추기 =="
# 진짜 flock 을 부르되 부른 횟수를 남긴다. 다시 실행한 스크립트가 잠금을 다시 잡으면(잠깐 풀리는 틈이 생김) 2번이 된다.
REAL_FLOCK=$(command -v flock)
printf '#!/bin/sh\necho flock >>"$W/flock.log"\nexec %s "$@"\n' "$REAL_FLOCK" >"$W/bin/flock"; chmod +x "$W/bin/flock"
reset_v1; setp standby "$V2"; : >"$W/flock.log"
run_apply scheduled 2026-11-11T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(head_of)" = "$V2" ] && [ "$(current)" = "$V2" ] && grep -q 'NEW-SCRIPT-RAN' "$W/out.log" && grep -q '저장소 사본을 맞춤' "$W/out.log"; } \
  && ok "C1 사본을 V2 커밋으로 맞추고 새 스크립트로 다시 실행해 적용" || bad "C1 (rc=$RC, head=$(head_of))"
[ "$(wc -l <"$W/flock.log")" -eq 1 ] && ok "C1b 다시 실행한 스크립트는 잠금을 다시 잡지 않고 넘겨받음(flock 1번)" || bad "C1b flock 을 $(wc -l <"$W/flock.log")번 부름"
rm -f "$W/bin/flock"
sync_fail() { # <이름> <설명>: 사본·서비스·지금 버전 모두 그대로인지 본다
  { [ "$RC" -ne 0 ] && [ "$(head_of)" = "$V1" ] && [ "$(current)" = "$V1" ] && ! grep -qE ' (pull|run|up) ' "$W/docker.log"; } \
    && ok "$1 $2: 사본·서비스를 건드리지 않고 실패" || bad "$1 (rc=$RC, head=$(head_of), current=$(current))"
}
reset_v1; setp standby "$V2"; echo '# 손으로 고침' >>"$W/repo/infra/deploy/compose/services.yml"
run_apply scheduled 2026-11-11T00:00:05+09:00; sync_fail C2 "사본에 손으로 고친 파일"
git -C "$W/repo" checkout -q -- .
reset_v1; touch "$W/repo/infra/deploy/server/local-note"
run_apply scheduled 2026-11-11T00:00:05+09:00; sync_fail C3 "사본에 새로 만든 파일"
rm -f "$W/repo/infra/deploy/server/local-note"
reset_v1; setp standby "$SIDE"
run_apply scheduled 2026-11-11T00:00:05+09:00; sync_fail C4 "기본 가지 이력에 없는 커밋"
reset_v1; setp standby "$V2"; git -C "$W/repo" remote set-url origin "$W/missing.git"
run_apply scheduled 2026-11-11T00:00:05+09:00; sync_fail C5 "fetch 실패"
git -C "$W/repo" remote set-url origin "$W/origin.git"
reset_v1; setp standby V9
run_apply scheduled 2026-11-11T00:00:05+09:00; sync_fail C6 "커밋 해시가 아닌 버전"
reset_v1; setp standby "$V10"; EXTRA=("${P}SYNCED_VERSION=$V2")
run_apply scheduled 2026-11-11T00:00:05+09:00; EXTRA=(); sync_fail C7 "다시 실행했는데 대상 버전이 달라짐(반복 방지)"
reset_v1; setp standby "$V3"
run_apply scheduled 2026-11-11T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(head_of)" = "$V3" ] && [ "$(current)" = "$V3" ]; } \
  && ok "C8 사본에 없던 커밋(V3)을 fetch 로 받아 맞추고 적용" || bad "C8 (rc=$RC, head=$(head_of))"
reset_v1; setp standby "$V2"; setp urgent "$V2"
run_apply urgent 2026-11-10T20:00:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(head_of)" = "$V2" ] && [ "$(current)" = "$V2" ] && grep -q 'NEW-SCRIPT-RAN' "$W/out.log" \
  && [ "$(cat "$STATE/urgent_seen" 2>/dev/null)" = "$V2" ]; } \
  && ok "C9 긴급 경로도 사본을 맞춰 다시 실행한 뒤 적용하고 본 요청으로 기록" || bad "C9 (rc=$RC, head=$(head_of))"

echo "== 복구 경로 =="
reset_v1; setp standby "$V2"
run_apply recover 2026-11-11T10:00:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V2" ] && grep -q ' up ' "$W/docker.log"; } \
  && ok "RC1 금지 시간(10:00)에도 대기 버전을 적용" || bad "RC1 (rc=$RC, current=$(current))"
: >"$W/docker.log"
run_apply recover 2026-11-11T10:05:00+09:00
{ [ "$RC" -eq 0 ] && grep -q ' up ' "$W/docker.log"; } && ok "RC2 지금 버전과 같아도 다시 띄움(멈춘 서비스 살리기)" || bad "RC2 (rc=$RC)"
reset_v1; rm -f "$W/param/_t_standby"
run_apply recover 2026-11-11T10:00:00+09:00
{ [ "$RC" -ne 0 ] && ! grep -q ' up ' "$W/docker.log"; } && ok "RC3 대기 버전이 없으면 실패로 끝남" || bad "RC3 (rc=$RC)"
reset_v1; setp standby "$V2"
run_apply scheduled 2026-11-11T10:00:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V1" ] && ! grep -q ' up ' "$W/docker.log"; } && ok "RC4 같은 시각의 정기 경로는 그대로 건너뜀" || bad "RC4 (rc=$RC)"

echo "== 긴급 경로 실패 =="
reset_v1; setp urgent "$U1"; setp standby "$U1"
run_apply urgent 2026-11-10T20:00:00+09:00 up
{ [ "$RC" -ne 0 ] && [ "$(current)" = "$V1" ] && [ ! -f "$STATE/urgent_seen" ]; } && ok "U1 다시 띄우기 실패: V1 유지, 본 요청으로 기록하지 않음" || bad "U1 (rc=$RC, current=$(current))"
run_apply urgent 2026-11-10T20:05:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$U1" ]; } && ok "U2 다음 확인에서 같은 요청을 다시 시도해 적용" || bad "U2 (rc=$RC, current=$(current))"

echo "== 긴급은 대기 버전을 앞당길 뿐 =="
reset_v1; setp standby "$V2"; setp urgent "$U9"
run_apply urgent 2026-11-10T20:00:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V1" ] && ! grep -q ' up' "$W/docker.log" && grep -q '대기 버전과 다름' "$W/out.log"; } \
  && ok "U3 긴급 U9 != 대기 V2: 적용하지 않고 이유를 로그에 남김" || bad "U3 (rc=$RC, current=$(current))"
[ "$(cat "$STATE/urgent_seen" 2>/dev/null)" = "$U9" ] && ok "U4 무시한 요청도 본 요청으로 기록" || bad "U4 urgent_seen 이 U9 가 아님"
setp standby "$U9"; : >"$W/docker.log"
run_apply urgent 2026-11-10T20:05:00+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V1" ] && ! grep -q ' up' "$W/docker.log"; } \
  && ok "U5 나중에 대기 버전이 U9 가 되어도 무시했던 긴급 값으로 낮에 바로 적용하지 않음" || bad "U5 (rc=$RC, current=$(current))"
run_apply scheduled 2026-11-11T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$U9" ]; } && ok "U5b 그 버전은 정기 배포가 적용" || bad "U5b (rc=$RC, current=$(current))"
setp standby "$V10"
run_apply scheduled 2026-11-12T00:00:05+09:00
{ [ "$RC" -eq 0 ] && [ "$(current)" = "$V10" ]; } && ok "U6 이후 정기 배포는 대기 버전을 적용(되돌림 없음: 대기 버전이 곧 최신)" || bad "U6 (rc=$RC, current=$(current))"

echo "== 이미지 정리 =="
reset_v1; setp standby "$V2"
printf '%s\n' r/stock-reading-market:V0 "r/stock-reading-market:$V1" "r/stock-reading-market:$V2" r/stock-reading-migrate:V0 other/image:V0 >"$W/images"
run_apply scheduled 2026-11-10T00:00:05+09:00
{ [ "$RC" -eq 0 ] && grep -q 'image rm r/stock-reading-market:V0' "$W/docker.log" && grep -q 'image rm r/stock-reading-migrate:V0' "$W/docker.log" \
  && ! grep -q "image rm r/stock-reading-market:$V1" "$W/docker.log" && ! grep -q "image rm r/stock-reading-market:$V2" "$W/docker.log" \
  && ! grep -q 'image rm other/image' "$W/docker.log"; } \
  && ok "G1 새 버전(V2)과 바로 전 버전(V1)만 남기고 이 프로젝트의 옛 이미지를 지움" || bad "G1 (rc=$RC)"
rm -f "$W/images"
reset_v1; run_apply scheduled 2026-11-10T00:00:05+09:00 up
grep -q 'image prune' "$W/docker.log" && bad "G2 다시 띄우기가 실패했는데 이미지를 정리함" || ok "G2 다시 띄우기 실패 때는 정리하지 않음"

echo "== 잠금 =="
reset_v1; setp standby "$V2"
# flock 이 잠금을 얻지 못하는 상황을 가짜 flock 으로 만든다(진짜는 15분을 기다려야 하므로)
printf '#!/bin/sh\nexit 1\n' >"$W/bin/flock"; chmod +x "$W/bin/flock"
run_apply scheduled 2026-11-10T00:00:05+09:00
{ [ "$RC" -eq 1 ] && ! grep -q ' up' "$W/docker.log"; } && ok "L1 정기 경로가 잠금을 못 얻으면 종료 코드 1(실패로 보임)" || bad "L1 (rc=$RC)"
rm -f "$W/bin/flock"

echo "== 비밀 설정(runtime.env) =="
RT() { cat "$STATE/runtime.env" 2>/dev/null; }
good_config() { # 필수 비밀번호 9개 + 특수문자 값
  {
    for n in POSTGRES_ADMIN_PASSWORD MARKET_DB_PASSWORD NEWS_DB_PASSWORD BRIEFING_DB_PASSWORD PERSONAL_OWNER_DB_PASSWORD PERSONAL_WRITER_DB_PASSWORD COLLECTOR_DB_PASSWORD NOTIFIER_DB_PASSWORD OPS_CHECKER_DB_PASSWORD; do
      printf '/cfg/%s\tpw-%s\n' "$n" "$n"
    done
    printf '/cfg/SMTP_PASSWORD\ta$b#c d\n'
  } >"$W/config_out"
}
cfg_apply() { EXTRA=("${P}CONFIG_PARAM_PATH=/cfg"); run_apply scheduled 2026-11-10T00:00:05+09:00; EXTRA=(); }
reset_v1; rm -f "$STATE/current"; setp standby "$V1"; good_config
cfg_apply
{ [ "$RC" -eq 0 ] && grep -qxF "SMTP_PASSWORD='a\$b#c d'" "$STATE/runtime.env" && grep -qxF "MARKET_DB_PASSWORD='pw-MARKET_DB_PASSWORD'" "$STATE/runtime.env"; } \
  && ok "R1 \$·#·공백이 든 값을 작은따옴표로 감싸 그대로 씀" || bad "R1 (rc=$RC) $(RT | grep SMTP)"
[ "$(stat -c %a "$STATE/runtime.env")" = 600 ] && ok "R2 runtime.env 권한 600" || bad "R2 권한"
reset_v1; rm -f "$STATE/current"; setp standby "$V1"; good_config; printf "/cfg/BAD\tit's\n" >>"$W/config_out"
cfg_apply
{ [ "$RC" -ne 0 ] && [ ! -f "$STATE/runtime.env" ] && [ "$(current)" = "" ] && ! grep -q ' up' "$W/docker.log"; } \
  && ok "R3 값에 작은따옴표: 실패로 멈추고 다시 띄우지 않음" || bad "R3 (rc=$RC)"
reset_v1; rm -f "$STATE/current"; setp standby "$V1"; : >"$W/config_out"
cfg_apply
{ [ "$RC" -ne 0 ] && [ ! -f "$STATE/runtime.env" ] && ! grep -q ' up' "$W/docker.log" && grep -q '설정이 비어 있음' "$W/out.log"; } \
  && ok "R4 받아 온 설정이 비어 있음: 실패로 멈추고 다시 띄우지 않음" || bad "R4 (rc=$RC)"
reset_v1; rm -f "$STATE/current"; setp standby "$V1"; good_config; grep -v 'COLLECTOR_DB_PASSWORD' "$W/config_out" >"$W/config_out.2"; mv "$W/config_out.2" "$W/config_out"
cfg_apply
{ [ "$RC" -ne 0 ] && ! grep -q ' up' "$W/docker.log" && grep -q 'COLLECTOR_DB_PASSWORD' "$W/out.log"; } \
  && ok "R5 필수 비밀번호가 없음: 실패로 멈추고 다시 띄우지 않음" || bad "R5 (rc=$RC)"
reset_v1; rm -f "$STATE/current"; setp standby "$V1"; good_config; printf '/cfg/MARKET_DB_PASSWORD\t\n' >>"$W/config_out"
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
