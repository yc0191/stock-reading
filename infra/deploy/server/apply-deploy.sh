#!/bin/bash
# 서비스 서버의 배포 적용 스크립트. 서비스(services.yml)만 새 이미지로 바꾸고 데이터 서버(Kafka·Postgres)는 건드리지 않는다.
#   apply-deploy.sh scheduled   매일 07:00(한국 시간) 정기 경로: Parameter Store 의 대기 버전을 읽는다.
#   apply-deploy.sh urgent      몇 분마다 도는 긴급 경로: Parameter Store 의 긴급 버전을 읽는다.
# 두 경로가 같은 금지 시간을 쓴다: 매일(주말 포함) 09:00:00~15:30:59 한국 시간에는 어떤 경로로도 적용하지 않는다.
# 금지 시간에 처음 본 긴급 값은 버린다(시간이 지나도 저절로 적용하지 않음). 서버는 Parameter Store 값을 읽기만 한다.
# 긴급은 "대기 버전을 07:00 까지 기다리지 않고 지금 적용"만 한다. 긴급 값이 대기 버전과 다르면 적용하지 않는다(이후 07:00 정기 배포가 되돌리지 않게).
#
# 시험용 환경변수(없으면 실제 값): DEPLOY_NOW(ISO 8601), DEPLOY_STATE_DIR, DEPLOY_STANDBY_PARAM, DEPLOY_URGENT_PARAM, DEPLOY_ENV_FILE.
set -euo pipefail

mode=${1:-}
case "$mode" in
  scheduled|urgent) ;;
  *) echo "사용법: $0 scheduled|urgent" >&2; exit 64 ;;
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
compose_file=${DEPLOY_COMPOSE_FILE:-$script_dir/../compose/services.yml}
state_dir=${DEPLOY_STATE_DIR:-/var/lib/stock-reading/deploy}
standby_param=${DEPLOY_STANDBY_PARAM:-/stock-reading/prod/deploy/standby-version}
urgent_param=${DEPLOY_URGENT_PARAM:-/stock-reading/prod/deploy/urgent-version}
runtime_env=${DEPLOY_RUNTIME_ENV:-$state_dir/runtime.env}

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
required_secrets="POSTGRES_ADMIN_PASSWORD MARKET_DB_PASSWORD NEWS_DB_PASSWORD BRIEFING_DB_PASSWORD PERSONAL_OWNER_DB_PASSWORD PERSONAL_WRITER_DB_PASSWORD COLLECTOR_DB_PASSWORD NOTIFIER_DB_PASSWORD"
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
apply_version() {
  version=$1
  log "적용 시작: 버전 $version"
  if [ -n "${ECR_REGISTRY:-}" ]; then
    aws ecr get-login-password ${AWS_REGION:+--region "$AWS_REGION"} \
      | docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null || { log "실패: ECR 로그인"; return 1; }
  fi
  render_runtime_env || return 1
  compose pull --quiet || { log "실패: 이미지 받기 (버전 $version)"; return 1; }
  # 받는 동안 금지 시간이 시작됐으면 여기서 멈춘다.
  if in_forbidden_window; then
    log "건너뜀: 이미지를 받는 사이 금지 시간이 되어 적용하지 않음 (버전 $version)"
    return 2
  fi
  compose up -d --remove-orphans || { log "실패: 서비스 다시 띄우기 (버전 $version), 지금 버전 기록은 그대로 둠"; return 1; }
  prune_old_images "$version" "${current:-}" || log "경고: 안 쓰는 이미지 정리 실패"
  printf '%s\n' "$version" >"$state_dir/current.tmp" && mv "$state_dir/current.tmp" "$state_dir/current" \
    || { log "실패: 지금 버전 기록"; return 1; }
  log "적용 끝: 버전 $version"
}

mkdir -p "$state_dir"
chmod 700 "$state_dir"
# 정기·긴급이 겹쳐 돌지 않게 한다.
exec 9>"$state_dir/lock"
if ! flock -w 900 9; then
  log "실패: 다른 배포가 15분 넘게 진행 중이라 잠금을 얻지 못함"
  exit 1
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
