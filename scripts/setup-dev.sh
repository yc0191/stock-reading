#!/usr/bin/env bash
# 새로 클론한 저장소나 새 작업 폴더에서 빠뜨리기 쉬운 로컬 준비 네 가지를 확인하고, 만들 수 있는 것은 만든다.
# - .env: 값은 사람이 채우므로 없거나 링크가 깨졌으면 알리기만 한다.
# - JDK 21: JAVA_HOME_21 이 JDK 21 을 가리키는지 본다. 셸 설정은 바꾸지 않고, 찾은 위치와 지정할 줄을 알려 준다.
# - 브리핑 venv: 없으면 만들고 requirements.txt 를 설치한다(있으면 설치만 다시 해 의존성 변경을 따라간다).
# - 차트 글꼴: scripts/install_font.py 로 받는다(이미 맞는 파일이 있으면 내려받지 않음).
#
# 사용법: scripts/setup-dev.sh [--check]
#   --check  아무것도 만들거나 받지 않고 상태만 본다
# 끝에 항목별 결과를 출력하고, 하나라도 준비되지 않았으면 1로 끝난다.
# 환경변수: SETUP_PYTHON  venv 를 만들 파이썬(기본 python3)
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 1

CHECK=0
case "${1:-}" in
  "") ;;
  --check) CHECK=1 ;;
  -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
  *) echo "사용법: $0 [--check]" >&2; exit 2 ;;
esac

PY_SYS="${SETUP_PYTHON:-python3}"
VENV="services/briefing/.venv"
RESULTS=()
status=0

report() {  # $1 = 항목, $2 = ok|bad, $3 = 설명
  local mark="준비됨"
  [ "$2" = ok ] || { mark="안 됨"; status=1; }
  RESULTS+=("- $1: $mark - $3")
}

# ---------------------------------------------------------------- .env

if [ -L .env ] && [ ! -e .env ]; then
  report ".env" bad "링크가 가리키는 파일이 없습니다. 링크를 다시 만드세요"
elif [ -f .env ]; then
  report ".env" ok "있음"
else
  report ".env" bad "없습니다. .env.example 을 복사해 값을 채우거나 쓰던 .env 를 링크하세요"
fi

# ---------------------------------------------------------------- JDK 21

is_jdk21() {
  # java_home -v 21 은 21이 없으면 다른 버전을 돌려주므로 실제 버전을 확인한다(test-all.sh 와 같은 방식).
  [ -n "$1" ] && [ -x "$1/bin/java" ] && "$1/bin/java" -version 2>&1 | head -1 | grep -qE 'version "21[."]'
}

if is_jdk21 "${JAVA_HOME_21:-}"; then
  report "JDK 21" ok "JAVA_HOME_21 이 JDK 21 을 가리킵니다"
else
  found=""
  candidates=()
  [ -x /usr/libexec/java_home ] && candidates+=("$(/usr/libexec/java_home -v 21 2>/dev/null || true)")
  command -v brew >/dev/null 2>&1 \
    && candidates+=("$(brew --prefix openjdk@21 2>/dev/null || true)/libexec/openjdk.jdk/Contents/Home")
  for candidate in ${candidates[@]+"${candidates[@]}"}; do
    is_jdk21 "$candidate" && { found=$candidate; break; }
  done
  if [ -n "$found" ]; then
    report "JDK 21" bad "JAVA_HOME_21 이 비었거나 JDK 21 이 아닙니다. 셸 설정에 추가하세요: export JAVA_HOME_21=\"$found\""
  else
    report "JDK 21" bad "JDK 21 을 찾지 못했습니다. 설치한 뒤 JAVA_HOME_21 에 그 위치를 지정하세요"
  fi
fi

# ---------------------------------------------------------------- 브리핑 venv

if [ "$CHECK" = 1 ]; then
  if [ -x "$VENV/bin/python" ]; then
    report "브리핑 venv" ok "있음(의존성 설치 여부는 --check 없이 실행해 맞춥니다)"
  else
    report "브리핑 venv" bad "없습니다"
  fi
else
  if [ ! -x "$VENV/bin/python" ]; then
    echo "브리핑 venv 만들기($VENV)"
    "$PY_SYS" -m venv "$VENV" || true
  fi
  if [ ! -x "$VENV/bin/python" ]; then
    report "브리핑 venv" bad "만들지 못했습니다($PY_SYS -m venv)"
  else
    echo "브리핑 의존성 설치"
    if "$VENV/bin/python" -m pip install -q --disable-pip-version-check -r services/briefing/requirements.txt; then
      report "브리핑 venv" ok "의존성 설치됨"
    else
      report "브리핑 venv" bad "의존성 설치에 실패했습니다(위 pip 출력 참고)"
    fi
  fi
fi

# ---------------------------------------------------------------- 차트 글꼴

if [ "$CHECK" = 1 ]; then
  # 파일 이름과 SHA-256 의 원본은 install_font.py 이므로 거기서 읽어 비교한다.
  if "$PY_SYS" -I -B - "$ROOT/scripts" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
import install_font as f
sys.exit(0 if all((f.FONT_DIR / n).is_file() and f.sha256((f.FONT_DIR / n).read_bytes()) == s
                  for n, s in f.FONTS.items()) else 1)
PY
  then
    report "차트 글꼴" ok "있음"
  else
    report "차트 글꼴" bad "없거나 내용이 다릅니다"
  fi
elif "$PY_SYS" scripts/install_font.py; then
  report "차트 글꼴" ok "있음"
else
  report "차트 글꼴" bad "내려받지 못했습니다(위 출력 참고)"
fi

# ---------------------------------------------------------------- 결과

echo "== 로컬 준비"
printf '%s\n' "${RESULTS[@]}"
exit "$status"
