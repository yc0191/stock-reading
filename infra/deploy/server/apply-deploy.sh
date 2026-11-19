#!/bin/bash
# 서비스 서버의 배포 적용 스크립트. 서비스(services.yml)만 새 이미지로 바꾸고 데이터 서버(Kafka·Postgres)는 건드리지 않는다.
#   apply-deploy.sh scheduled   매일 07:00(한국 시간) 정기 경로: Parameter Store 의 대기 버전을 읽는다.
#   apply-deploy.sh urgent      몇 분마다 도는 긴급 경로: Parameter Store 의 긴급 버전을 읽는다.
#   apply-deploy.sh recover     사람이 직접 실행하는 복구 경로: 금지 시간과 상관없이 대기 버전을 적용한다(멈춘 서비스를 장중에 살릴 때).
# 정기·긴급 경로가 같은 금지 시간을 쓴다: 매일(주말 포함) 09:00:00~15:30:59 한국 시간에는 어떤 경로로도 적용하지 않는다.
# 금지 시간에 처음 본 긴급 값은 버린다(시간이 지나도 저절로 적용하지 않음). 서버는 Parameter Store 값을 읽기만 한다.
# 긴급은 "대기 버전을 07:00 까지 기다리지 않고 지금 적용"만 한다. 긴급 값이 대기 버전과 다르면 적용하지 않는다(이후 07:00 정기 배포가 되돌리지 않게).
# 적용 전에 이 스크립트가 든 저장소 사본을 적용할 버전 커밋으로 맞추고, 맞췄으면 새 스크립트로 처음부터 다시 실행한다.
# Compose 파일과 이 스크립트가 이미지와 같은 버전이 되게 하려는 것이다. 사본에 손으로 고치거나 새로 만든 파일이 있으면 맞추지 않고 실패로 끝낸다.
#
# 시험용 환경변수(없으면 실제 값): DEPLOY_NOW(ISO 8601), DEPLOY_STATE_DIR, DEPLOY_STANDBY_PARAM, DEPLOY_URGENT_PARAM, DEPLOY_ENV_FILE.
set -euo pipefail

mode=${1:-}
case "$mode" in
  scheduled|urgent|recover) ;;
  *) echo "사용법: $0 scheduled|urgent|recover" >&2; exit 64 ;;
esac

# 설정 파일의 값은 이미 환경변수로 준 값을 덮어쓰지 않는다(시험·수동 실행에서 바꿔 넣을 수 있게).
env_file=${DEPLOY_ENV_FILE:-/etc/stock-reading/deploy.env}
override=()
for name in DEPLOY_NOW DEPLOY_STATE_DIR DEPLOY_STANDBY_PARAM DEPLOY_URGENT_PARAM; do
  [ -n "${!name+x}" ] && override+=("$name=${!name}")
done
if [ -f "$env_file" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$env_file"
  set +a
fi
for pair in "${override[@]}"; do export "${pair?}"; done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
script_path=$script_dir/$(basename "${BASH_SOURCE[0]}")
repo_dir=$(cd "$script_dir/../../.." && pwd)
compose_file=${DEPLOY_COMPOSE_FILE:-$script_dir/../compose/services.yml}
state_dir=${DEPLOY_STATE_DIR:-/var/lib/stock-reading/deploy}
standby_param=${DEPLOY_STANDBY_PARAM:-/stock-reading/prod/deploy/standby-version}
urgent_param=${DEPLOY_URGENT_PARAM:-/stock-reading/prod/deploy/urgent-version}
runtime_env=${DEPLOY_RUNTIME_ENV:-$state_dir/runtime.env}
migrate_service=flyway-migrate

log() { echo "$(date -u +%FT%TZ) deploy[$mode] $*"; }

# 한국 시간의 시분초(HHMMSS). 서버 시간대와 상관없이 한국 시간으로 본다.
kst_hms() {
  if [ -n "${DEPLOY_NOW:-}" ]; then
    TZ=Asia/Seoul date -d "$DEPLOY_NOW" +%H%M%S
  else
    TZ=Asia/Seoul date +%H%M%S
  fi
}

# 09:00:00 이상 15:30:59 이하(10진수로 비교하려고 앞의 0을 뗀다).
in_forbidden_window() {
  local hms
  hms=$(kst_hms)
  hms=$((10#$hms))
  [ "$hms" -ge 90000 ] && [ "$hms" -le 153059 ]
}

# Parameter Store 값을 읽는다. 값이 없으면 빈 문자열, 그 밖의 오류는 실패로 끝낸다.
read_param() {
  local out err
  err=$(mktemp)
  if out=$(aws ssm get-parameter --name "$1" --query Parameter.Value --output text \
      ${AWS_REGION:+--region "$AWS_REGION"} 2>"$err"); then
    rm -f "$err"
    [ "$out" = "None" ] && out=""
    printf '%s' "$out"
    return 0
  fi
  if grep -q 'ParameterNotFound' "$err"; then
    rm -f "$err"
    return 0
  fi
  log "Parameter Store 읽기 실패: $1 ($(head -c 300 "$err" | tr '\n' ' '))" >&2
  rm -f "$err"
  return 1
}

valid_version() { [[ "$1" =~ ^[A-Za-z0-9._-]{1,128}$ ]]; }

# 서비스에 넘길 비밀 설정을 Parameter Store 에서 받아 서버 안 파일(0600)에만 둔다. 이름 하나가 환경변수 하나다.
# 경로를 정하지 않은 서버(DEPLOY_CONFIG_PARAM_PATH 가 비어 있음)는 건너뛴다. 경로를 정했는데 결과가 비었거나 필수 값이 비면 실패로 멈춘다.
# 값은 작은따옴표로 감싸 쓴다: Compose 가 값 안의 $ 를 변수로 풀고 ' #' 뒤를 주석으로 보기 때문이다. 값에 작은따옴표가 있으면 감쌀 수 없어 실패로 멈춘다.
required_secrets="POSTGRES_ADMIN_PASSWORD MARKET_DB_PASSWORD NEWS_DB_PASSWORD BRIEFING_DB_PASSWORD PERSONAL_OWNER_DB_PASSWORD PERSONAL_WRITER_DB_PASSWORD COLLECTOR_DB_PASSWORD NOTIFIER_DB_PASSWORD OPS_CHECKER_DB_PASSWORD"
render_runtime_env() {
  [ -n "${DEPLOY_CONFIG_PARAM_PATH:-}" ] || return 0
  local tmp name
  tmp=$(mktemp "$state_dir/runtime.env.XXXXXX")
  chmod 600 "$tmp"
  if ! aws ssm get-parameters-by-path --path "$DEPLOY_CONFIG_PARAM_PATH" --with-decryption \
      --query 'Parameters[].[Name,Value]' --output text ${AWS_REGION:+--region "$AWS_REGION"} \
      | while IFS=$'\t' read -r name value; do
          [ -n "$name" ] || continue
          case "$value" in
            *"'"*) echo "값에 작은따옴표가 있어 쓸 수 없음: ${name##*/}" >&2; exit 1 ;;
          esac
          printf "%s='%s'\n" "${name##*/}" "$value"
        done >"$tmp"; then
    rm -f "$tmp"
    log "설정 읽기 실패: $DEPLOY_CONFIG_PARAM_PATH"
    return 1
  fi
  if [ ! -s "$tmp" ]; then
    rm -f "$tmp"
    log "설정이 비어 있음(경로 확인 필요): $DEPLOY_CONFIG_PARAM_PATH"
    return 1
  fi
  for name in $required_secrets; do
    if ! grep -Eq "^${name}='.+'\$" "$tmp"; then
      rm -f "$tmp"
      log "필수 설정이 없거나 비어 있음: $name"
      return 1
    fi
  done
  mv "$tmp" "$runtime_env"
}

# 저장소 사본을 버전 커밋으로 맞춘다. 반환: 0 이미 맞음, 1 실패(사본은 그대로), 3 맞췄음(새 스크립트로 다시 실행해야 함).
# 해시로 바로 fetch 하지 않고 main 을 받아 그 이력에 있는 커밋만 쓴다. GitHub 는 포크의 커밋도 해시로 내주기 때문이다.
sync_repo() {
  local sha=$1 head dirty
  if ! [[ "$sha" =~ ^[0-9a-f]{40}$ ]]; then
    log "실패: 버전이 커밋 해시가 아니라 저장소 사본을 맞출 수 없음 (버전 $sha)"
    return 1
  fi
  dirty=$(git -C "$repo_dir" status --porcelain) || { log "실패: 저장소 사본 상태 확인 ($repo_dir)"; return 1; }
  if [ -n "$dirty" ]; then
    log "실패: 저장소 사본에 손으로 고치거나 새로 만든 파일이 있어 맞추지 않음 ($repo_dir, $(printf '%s' "$dirty" | head -5 | tr '\n' ' '))"
    return 1
  fi
  head=$(git -C "$repo_dir" rev-parse HEAD) || { log "실패: 저장소 사본의 지금 커밋 확인"; return 1; }
  [ "$head" = "$sha" ] && return 0
  # 다시 실행한 스크립트가 또 맞추려 하면(그 사이 대상이 바뀜) 반복하지 않고 멈춘다. 다음 실행이 다시 시도한다.
  if [ -n "${DEPLOY_SYNCED_VERSION:-}" ]; then
    log "실패: 사본을 맞춰 다시 실행했는데 대상 버전이 달라짐 (맞춘 버전 $DEPLOY_SYNCED_VERSION, 대상 $sha)"
    return 1
  fi
  # 네트워크가 멈춰도 잠금을 오래 쥐지 않게 시간을 제한한다.
  timeout 120 git -C "$repo_dir" fetch --quiet origin main || { log "실패: 저장소 사본 fetch"; return 1; }
  if ! git -C "$repo_dir" cat-file -e "$sha^{commit}" 2>/dev/null \
      || ! git -C "$repo_dir" merge-base --is-ancestor "$sha" FETCH_HEAD; then
    log "실패: 버전 커밋이 main 이력에 없음 (버전 $sha)"
    return 1
  fi
  git -C "$repo_dir" -c advice.detachedHead=false checkout --quiet --detach "$sha" \
    || { log "실패: 저장소 사본을 버전 커밋으로 바꾸기 (버전 $sha)"; return 1; }
  log "저장소 사본을 맞춤: ${head:0:12} -> ${sha:0:12}"
  return 3
}

compose() {
  local args=(-f "$compose_file")
  [ -f "$env_file" ] && args+=(--env-file "$env_file")
  [ -f "$runtime_env" ] && args+=(--env-file "$runtime_env")
  DEPLOY_VERSION=$version docker compose "${args[@]}" "$@"
}

# 이 프로젝트 이미지 중 새 버전과 바로 전 버전(되돌리기용)만 남기고 지운다.
# 날짜 필터(until)는 배포한 날이 아니라 빌드한 날 기준이라 바로 전 버전까지 지울 수 있어 쓰지 않는다.
prune_old_images() {
  local keep_new=$1 keep_prev=$2 ref tag
  while IFS= read -r ref; do
    case "$ref" in *stock-reading-*:*) ;; *) continue ;; esac
    tag=${ref##*:}
    if [ "$tag" = "$keep_new" ] || { [ -n "$keep_prev" ] && [ "$tag" = "$keep_prev" ]; }; then
      continue
    fi
    docker image rm "$ref" </dev/null >/dev/null || return 1
  done < <(docker image ls --format '{{.Repository}}:{{.Tag}}')
  docker image prune -f >/dev/null
}

# 새 버전으로 바꾼다. 중간에 실패하면 지금 버전 기록은 그대로 둔다.
# 이 함수는 `|| rc=$?` 안에서 불려 set -e 가 꺼지므로, 단계마다 실패를 직접 확인한다. 반환: 0 적용, 1 실패, 2 금지 시간이 되어 멈춤.
# 사본을 맞췄으면 돌아오지 않고 새 스크립트로 바뀐다(exec). 실행 중인 스크립트 파일이 바뀐 뒤 옛 실행을 이어 가지 않으려는 것이다.
# 바뀐 스크립트는 처음부터 다시 판단한다. 그 전까지는 아무 기록도 쓰지 않았으므로 판단 결과가 같다. 잠금(fd 9)은 넘겨받는다.
apply_version() {
  version=$1
  local rc=0 svc svc_list
  local services=()
  sync_repo "$version" || rc=$?
  if [ "$rc" -eq 3 ]; then
    export DEPLOY_SYNCED_VERSION=$version DEPLOY_LOCK_INHERITED=1
    exec bash "$script_path" "$mode"
  fi
  [ "$rc" -eq 0 ] || return 1
  log "적용 시작: 버전 $version"
  if [ -n "${ECR_REGISTRY:-}" ]; then
    aws ecr get-login-password ${AWS_REGION:+--region "$AWS_REGION"} \
      | docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null || { log "실패: ECR 로그인"; return 1; }
  fi
  render_runtime_env || return 1
  compose pull --quiet || { log "실패: 이미지 받기 (버전 $version)"; return 1; }
  # 받는 동안 금지 시간이 시작됐으면 여기서 멈춘다(복구 경로는 금지 시간에 쓰려고 만든 것이라 보지 않는다).
  if [ "$mode" != recover ] && in_forbidden_window; then
    log "건너뜀: 이미지를 받는 사이 금지 시간이 되어 적용하지 않음 (버전 $version)"
    return 2
  fi
  # DB 표 만들기를 서비스와 따로 먼저 돌린다. 함께 up 하면 Compose 가 옛 서비스를 먼저 지우므로, 표 만들기가 실패하면 모든 서비스가 멈춘다.
  compose run --rm -T --no-deps "$migrate_service" \
    || { log "실패: DB 표 만들기 (버전 $version), 서비스는 바꾸지 않음(옛 버전이 계속 돎)"; return 1; }
  svc_list=$(compose config --services) || { log "실패: Compose 파일에서 서비스 목록을 읽지 못함"; return 1; }
  while IFS= read -r svc; do
    [ -n "$svc" ] && [ "$svc" != "$migrate_service" ] && services+=("$svc")
  done <<<"$svc_list"
  [ "${#services[@]}" -gt 0 ] || { log "실패: Compose 파일에 서비스가 없음"; return 1; }
  # --no-deps: up 이 depends_on 을 따라 표 만들기를 한 번 더 돌리지 않게 한다(방금 성공했다).
  compose up -d --no-deps --remove-orphans "${services[@]}" \
    || { log "실패: 서비스 다시 띄우기 (버전 $version), 지금 버전 기록은 그대로 둠"; return 1; }
  prune_old_images "$version" "${current:-}" || log "경고: 안 쓰는 이미지 정리 실패"
  printf '%s\n' "$version" >"$state_dir/current.tmp" && mv "$state_dir/current.tmp" "$state_dir/current" \
    || { log "실패: 지금 버전 기록"; return 1; }
  log "적용 끝: 버전 $version"
}

mkdir -p "$state_dir"
chmod 700 "$state_dir"
# 정기·긴급·복구가 겹쳐 돌지 않게 한다. 사본을 맞춘 뒤 다시 실행한 스크립트는 이미 쥔 잠금을 그대로 쓴다(다시 열면 잠깐 풀린다).
if [ "${DEPLOY_LOCK_INHERITED:-}" = 1 ] && { true >&9; } 2>/dev/null; then
  :
else
  exec 9>"$state_dir/lock"
  if ! flock -w 900 9; then
    log "실패: 다른 배포가 15분 넘게 진행 중이라 잠금을 얻지 못함"
    exit 1
  fi
fi

current=""
[ -f "$state_dir/current" ] && current=$(<"$state_dir/current")

if [ "$mode" = scheduled ]; then
  if in_forbidden_window; then
    log "건너뜀: 금지 시간(한국 시간 09:00~15:30)"
    exit 0
  fi
  target=$(read_param "$standby_param")
  if [ -z "$target" ]; then
    log "건너뜀: 대기 버전 값이 없음"
    exit 0
  fi
  valid_version "$target" || { log "건너뜀: 대기 버전 값의 모양이 올바르지 않음"; exit 0; }
  if [ "$target" = "$current" ]; then
    log "건너뜀: 대기 버전이 지금 버전과 같음 ($current)"
    exit 0
  fi
  rc=0
  apply_version "$target" || rc=$?
  [ "$rc" -eq 2 ] && exit 0
  exit "$rc"
fi

if [ "$mode" = recover ]; then
  # 사람이 직접 실행하므로 건너뛸 이유가 있으면 실패로 끝내 눈에 띄게 한다. 지금 버전과 같아도 적용한다(멈춘 서비스를 다시 띄운다).
  target=$(read_param "$standby_param")
  if [ -z "$target" ] || ! valid_version "$target"; then
    log "실패: 대기 버전 값이 없거나 모양이 올바르지 않음"
    exit 1
  fi
  log "복구: 금지 시간과 상관없이 대기 버전을 적용 (지금 버전 ${current:-없음}, 대기 $target)"
  rc=0
  apply_version "$target" || rc=$?
  exit "$rc"
fi

# 긴급 경로
target=$(read_param "$urgent_param")
if [ -z "$target" ]; then
  log "건너뜀: 긴급 버전 값이 없음"
  exit 0
fi
valid_version "$target" || { log "건너뜀: 긴급 버전 값의 모양이 올바르지 않음"; exit 0; }
seen=""
[ -f "$state_dir/urgent_seen" ] && seen=$(<"$state_dir/urgent_seen")
if [ "$target" = "$seen" ]; then
  log "건너뜀: 이미 본 긴급 요청 ($target)"
  exit 0
fi
if in_forbidden_window; then
  # 금지 시간에 처음 본 요청은 기억해 두어, 15:30이 지나도 적용하지 않는다.
  printf '%s\n' "$target" >"$state_dir/urgent_seen"
  log "버림: 금지 시간(한국 시간 09:00~15:30)에 들어온 긴급 요청 ($target)"
  exit 0
fi
if [ "$target" = "$current" ]; then
  printf '%s\n' "$target" >"$state_dir/urgent_seen"
  log "건너뜀: 긴급 버전이 지금 버전과 같음 ($current)"
  exit 0
fi
# 긴급은 대기 버전을 앞당겨 적용하는 것뿐이다. 대기 버전과 다르면 다음 07:00 정기 배포가 되돌리므로 적용하지 않는다.
# 본 요청으로 기록해, 나중에 대기 버전이 우연히 이 값이 되어도 낮에 바로 적용하지 않는다(그때는 07:00 정기 배포가 적용).
standby=$(read_param "$standby_param")
if [ "$target" != "$standby" ]; then
  printf '%s\n' "$target" >"$state_dir/urgent_seen"
  log "버림: 긴급 버전이 대기 버전과 다름(긴급 $target, 대기 ${standby:-없음})"
  exit 0
fi
rc=0
apply_version "$target" || rc=$?
if [ "$rc" -eq 0 ]; then
  printf '%s\n' "$target" >"$state_dir/urgent_seen"
fi
[ "$rc" -eq 2 ] && exit 0
exit "$rc"
