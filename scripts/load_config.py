"""Parameter Store 의 notifier·collector 설정을 읽어 .env 에 반영한다. 값은 어디에도 출력하지 않는다."""
import argparse
import json
import os
import stat
import subprocess
import sys

PREFIX = "/stock-reading/dev/notifier/"
COLLECTOR_PREFIX = "/stock-reading/dev/collector/"
DEFAULT_REGION = "ap-southeast-2"

# 파라미터 이름 -> 환경변수 이름. 여기에 없는 이름은 읽어도 반영하지 않는다.
# 발송 스위치(NOTIFIER_SEND_ENABLED)는 일부러 넣지 않는다: 실수로 켜지지 않도록 .env 에서만 사람이 바꾼다.
ENV_NAMES = {
    "smtp-host": "SMTP_HOST",
    "smtp-port": "SMTP_PORT",
    "smtp-username": "SMTP_USERNAME",
    "smtp-password": "SMTP_PASSWORD",
    "sender": "NOTIFIER_SENDER",
    "recipient": "NOTIFIER_RECIPIENT",
}

# collector 는 선택 그룹이다: 없어도 notifier 설정 반영을 막지 않는다.
COLLECTOR_ENV_NAMES = {
    "dart-api-key": "DART_API_KEY",
    "kb-app-key": "BROKER_APP_KEY",
    "kb-app-secret": "BROKER_APP_SECRET",
    "naver-client-id": "NEWS_SEARCH_CLIENT_ID",
    "naver-client-secret": "NEWS_SEARCH_CLIENT_SECRET",
}


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


def fetch_parameters(profile, region, run=subprocess.run, prefix=PREFIX):
    command = ["aws", "ssm", "get-parameters-by-path", "--path", prefix.rstrip("/"), "--with-decryption",
               "--output", "json", "--region", region]
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
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않고 반영될 키 이름만 보여 준다")
    args = parser.parse_args(argv)

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

    try:
        collector_updates, _ = to_env_updates(
            fetch_parameters(args.profile, args.region, run, COLLECTOR_PREFIX), COLLECTOR_PREFIX, COLLECTOR_ENV_NAMES)
    except (RuntimeError, ValueError) as error:
        print("중단:", error)
        return 1
    updates.update(collector_updates)
    for short, env_name in COLLECTOR_ENV_NAMES.items():
        if env_name not in collector_updates:
            print(f"참고: collector/{short} 가 없어 {env_name} 은 반영하지 않습니다")

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
