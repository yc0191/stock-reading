"""Parameter Store 의 notifier·collector·briefing 설정을 읽어 .env 에 반영한다. 값은 어디에도 출력하지 않는다."""
import argparse
import json
import os
import stat
import subprocess
import sys

PREFIX = "/stock-reading/dev/notifier/"
COLLECTOR_PREFIX = "/stock-reading/dev/collector/"
BRIEFING_PREFIX = "/stock-reading/dev/briefing/"

# 파라미터 이름 -> 환경변수 이름. 여기에 없는 이름은 읽어도 반영하지 않는다.
# 발송 스위치(NOTIFIER_SEND_ENABLED)는 일부러 넣지 않는다: 실수로 켜지지 않도록 .env 에서만 사람이 바꾼다.
ENV_NAMES = {
    "smtp-host": "SMTP_HOST",
    "smtp-port": "SMTP_PORT",
    "smtp-username": "SMTP_USERNAME",
    "smtp-password": "SMTP_PASSWORD",
    "sender": "NOTIFIER_SENDER",
    # 앱은 받는 주소를 읽지 않는다(DB 의 member_mail 에서 고름). 전체 실행 스크립트(test-all.sh --live)가
    # 테스트 DB 의 가짜 멤버에 넣을 시험용 주소로만 쓴다.
    "recipient": "TEST_ALL_LIVE_RECIPIENT",
}

# collector 는 선택 그룹이다: 없어도 notifier 설정 반영을 막지 않는다.
COLLECTOR_ENV_NAMES = {
    "dart-api-key": "DART_API_KEY",
    "kb-app-key": "BROKER_APP_KEY",
    "kb-app-secret": "BROKER_APP_SECRET",
    "broker-base-url": "BROKER_BASE_URL",
    # 전체 실행 스크립트(test-all.sh --live)가 실제 시세를 부를 기준 종목 하나.
    "test-live-symbol": "TEST_ALL_LIVE_SYMBOL",

    "naver-client-id": "NEWS_SEARCH_CLIENT_ID",
    "naver-client-secret": "NEWS_SEARCH_CLIENT_SECRET",
    "news-search-base-url": "NEWS_SEARCH_BASE_URL",
    "news-search-id-header": "NEWS_SEARCH_ID_HEADER",
    "news-search-secret-header": "NEWS_SEARCH_SECRET_HEADER",
}

# briefing 도 선택 그룹이다: 키가 없으면 브리핑이 규칙 기반 리포트로 동작하므로 다른 그룹 반영을 막지 않는다.
BRIEFING_ENV_NAMES = {
    "anthropic-api-key": "ANTHROPIC_API_KEY",
}

# 선택 그룹: (--only 이름, 경로, 매핑). 순서대로 가져온다.
OPTIONAL_GROUPS = (
    ("collector", COLLECTOR_PREFIX, COLLECTOR_ENV_NAMES),
    ("briefing", BRIEFING_PREFIX, BRIEFING_ENV_NAMES),
)

# --only로 한 그룹만 확인해도 다른 그룹이 관리하는 이름을 낡은 이름으로 오인하지 않도록 전체 목록과 비교한다.
MANAGED_ENV_NAMES = frozenset(ENV_NAMES.values()).union(
    *(env_names.values() for _, _, env_names in OPTIONAL_GROUPS))


def to_env_updates(parameters, prefix=PREFIX, env_names=ENV_NAMES):
    """파라미터 목록을 (환경변수 값 사전, 무시한 이름 목록)으로 바꾼다."""
    updates, ignored = {}, []
    for parameter in parameters:
        name = parameter["Name"]
        short = name[len(prefix):] if name.startswith(prefix) else name
        env_name = env_names.get(short)
        if env_name is None:
            ignored.append(short)
            continue
        value = parameter["Value"]
        # 줄바꿈이 섞이면 .env 에 다른 키가 끼어들 수 있어 거부한다.
        if "\n" in value or "\r" in value:
            raise ValueError(f"{short}: 값에 줄바꿈이 있어 반영하지 않습니다")
        updates[env_name] = value
    return updates, ignored


def missing_names(updates):
    return sorted(short for short, env_name in ENV_NAMES.items() if env_name not in updates)


def merge_env(text, updates):
    """기존 .env 내용에서 updates 의 키만 바꾸거나 덧붙이고 나머지는 그대로 둔다."""
    remaining = dict(updates)
    lines = []
    for line in text.splitlines():
        key = line.split("=", 1)[0].strip() if "=" in line and not line.lstrip().startswith("#") else None
        if key in remaining:
            lines.append(f"{key}={remaining.pop(key)}")
        else:
            lines.append(line)
    lines.extend(f"{key}={value}" for key, value in remaining.items())
    return "\n".join(lines) + "\n"


def parse_env(text):
    """주석과 빈 줄을 뺀 NAME=값 줄을 사전으로 읽는다. 값은 따옴표를 벗기지 않고 그대로 둔다."""
    values = {}
    for line in text.splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value
    return values


def compare_env(text, updates):
    """Parameter Store 값(updates)과 .env 값을 이름별로 비교한다. 값은 돌려주지 않고 상태와 길이만 돌려준다."""
    local = parse_env(text)
    rows = []
    for name in sorted(updates):
        remote_value = updates[name]
        if name not in local:
            status = "없음"
        elif local[name] == remote_value:
            status = "같음"
        else:
            status = "다름"
        rows.append((name, status, len(local[name]) if name in local else None, len(remote_value)))
    return rows


def unmapped_credential_names(text, updates):
    """매핑되지 않은 앱키·시크릿 이름을 찾는다. 옛 이름이 남아 낡은 값을 쓰는 일을 알아채기 위해서다."""
    return sorted(name for name in parse_env(text)
                  if ("APP_KEY" in name or "APP_SECRET" in name)
                  and name not in updates and name not in MANAGED_ENV_NAMES)


def fetch_parameters(profile, region, run=subprocess.run, prefix=PREFIX):
    command = ["aws", "ssm", "get-parameters-by-path", "--path", prefix.rstrip("/"), "--with-decryption",
               "--output", "json"] + (["--region", region] if region else [])
    # 프로필을 주지 않으면 AWS CLI 기본 자격증명 방식(AWS_PROFILE 등)을 그대로 따른다.
    if profile:
        command += ["--profile", profile]
    result = run(command, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        # AWS 오류 문구에는 계정 번호가 들어 있어 그대로 출력하지 않는다.
        raise RuntimeError("Parameter Store 읽기 실패 (자격증명·지역·권한을 확인하세요)")
    return json.loads(result.stdout)["Parameters"]


def write_atomic(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
    os.replace(tmp, path)


def main(argv=None, run=subprocess.run):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--profile", help="AWS CLI 프로필. 없으면 기본 자격증명 방식(AWS_PROFILE 등)을 쓴다")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION"),
                        help="리전. 없으면 AWS_REGION, 그것도 없으면 프로필에 정한 리전을 쓴다")
    # 뉴스 검증처럼 한 그룹의 키만 필요할 때, 다른 그룹의 비밀이 .env 에 들어오지 않게 한다.
    parser.add_argument("--only", choices=["notifier", "collector", "briefing"],
                        help="한 그룹만 가져온다. 없으면 모두 가져온다")
    parser.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않고 반영될 키 이름만 보여 준다")
    # .env 는 복사본이라 Parameter Store 와 어긋날 수 있다. 실서버를 호출하기 전에 값을 출력하지 않고 비교한다.
    parser.add_argument("--check", action="store_true",
                        help="파일을 쓰지 않고 .env 와 Parameter Store 를 이름별로 비교한다(값은 출력하지 않음)")
    args = parser.parse_args(argv)

    updates = {}
    if args.only in (None, "notifier"):
        try:
            updates, ignored = to_env_updates(fetch_parameters(args.profile, args.region, run))
        except (RuntimeError, ValueError) as error:
            print("중단:", error)
            return 1

        if ignored:
            print("무시한 파라미터:", ", ".join(sorted(ignored)))
        missing = missing_names(updates)
        if missing:
            # 일부만 반영하면 로컬이 어정쩡한 상태가 되므로 아무것도 쓰지 않는다.
            print("중단: 필요한 파라미터가 없습니다 ->", ", ".join(missing))
            return 1

    for group, prefix, env_names in OPTIONAL_GROUPS:
        if args.only not in (None, group):
            continue
        try:
            group_updates, _ = to_env_updates(
                fetch_parameters(args.profile, args.region, run, prefix), prefix, env_names)
        except (RuntimeError, ValueError) as error:
            print("중단:", error)
            return 1
        updates.update(group_updates)
        for short, env_name in env_names.items():
            if env_name not in group_updates:
                print(f"참고: {group}/{short} 가 없어 {env_name} 은 반영하지 않습니다")

    if args.check:
        text = open(args.env_file, encoding="utf-8").read() if os.path.exists(args.env_file) else ""
        mismatched = False
        for name, status, local_length, remote_length in compare_env(text, updates):
            if status != "같음":
                mismatched = True
            local_text = "-" if local_length is None else f"{local_length}자"
            print(f"{name}: {status} (.env {local_text} / Parameter Store {remote_length}자)")
        for name in unmapped_credential_names(text, updates):
            mismatched = True
            print(f"경고: {name} 은 로더가 관리하지 않는 이름입니다. 낡은 값이면 지우거나 갱신하세요")
        print("확인 결과:", "어긋남이 있습니다" if mismatched else "모두 같습니다")
        return 1 if mismatched else 0

    print("반영할 키:", ", ".join(sorted(updates)))
    if args.dry_run:
        print("dry-run: 파일은 쓰지 않았습니다")
        return 0

    old = open(args.env_file, encoding="utf-8").read() if os.path.exists(args.env_file) else ""
    write_atomic(args.env_file, merge_env(old, updates))
    print(f"{args.env_file} 갱신 완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
