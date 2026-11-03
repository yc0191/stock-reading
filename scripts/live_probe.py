"""시세 제공자의 운영 서버를 호출하는 호출 속도 시험(BrokerRateProbeTest)을 안전하게 실행한다.

- 키는 .env 가 아니라 Parameter Store 에서 직접 읽어 시험 프로세스의 환경변수로만 넘긴다. 명령줄 인자와 출력에는 남기지 않는다.
- 직전 시험이 토큰 거절로 끝났으면 표식을 지우기 전에는 다시 실행하지 않는다(틀린 키를 반복해서 보내지 않기 위해).
- 직전 시험이 이상 응답(한도 초과 등)으로 멈췄으면 쉬는 시간이 지나기 전에는 다시 실행하지 않는다.
- 시험 클래스는 이 도구가 켜 주는 LIVE_PROBE_GUARD 가 있을 때만 실행된다. 손으로 gradlew 를 돌리면 건너뛴다.
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import load_config as lc

GUARD_ENV = "LIVE_PROBE_GUARD"
MARKER_NAME = ".live-probe-last.json"
DEFAULT_COOLDOWN_MINUTES = 10
TEST_CLASS = "BrokerRateProbeTest"
SMOKE_TEST_CLASS = "BrokerClientLiveSmokeTest"

OUTCOME_OK = "ok"
OUTCOME_TOKEN_REJECTED = "token_rejected"
OUTCOME_ABNORMAL = "abnormal"
OUTCOME_TEXT = {OUTCOME_OK: "정상 종료", OUTCOME_TOKEN_REJECTED: "토큰 거절", OUTCOME_ABNORMAL: "이상 응답으로 중단"}


def classify_smoke(output_text):
    """연동 확인 시험의 결과. 토큰 때문에 실패했으면 거절, 그 밖의 호출 실패는 이상 응답이다."""
    if "type=TOKEN_ISSUE_FAILED" in output_text or not output_text.strip():
        return OUTCOME_TOKEN_REJECTED
    if ": FAIL" in output_text:
        return OUTCOME_ABNORMAL
    return OUTCOME_OK


def classify(output_text):
    """시험이 남긴 결과 파일 내용으로 끝난 모양을 가른다."""
    first = next((line for line in output_text.splitlines() if line.startswith("token:")), "")
    if "http=200" not in first:
        return OUTCOME_TOKEN_REJECTED
    if "FIRST ABNORMAL" in output_text:
        return OUTCOME_ABNORMAL
    return OUTCOME_OK


def read_marker(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_marker(path, outcome, now):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"outcome": outcome, "at": now.isoformat()}, f)


def refusal(marker, now, cooldown_minutes, force):
    """다시 실행해도 되는지 판단한다. 거부해야 하면 이유를, 해도 되면 None 을 돌려준다."""
    if marker is None:
        return None
    if marker["outcome"] == OUTCOME_TOKEN_REJECTED:
        # 틀린 키를 반복해서 보내지 않도록 --force 로도 넘을 수 없고, 키를 확인한 사람이 표식을 지워야 한다.
        return ("직전 시험에서 토큰 발급이 거절되었습니다. 키를 확인한 뒤 "
                f"{MARKER_NAME} 파일을 지우고 다시 실행하세요")
    if marker["outcome"] == OUTCOME_ABNORMAL and not force:
        ready_at = datetime.fromisoformat(marker["at"]) + timedelta(minutes=cooldown_minutes)
        if now < ready_at:
            minutes = int((ready_at - now).total_seconds() // 60) + 1
            return f"직전 시험이 이상 응답으로 멈췄습니다. 약 {minutes}분 뒤에 다시 실행하세요(--force 로 넘길 수 있음)"
    return None


def credentials_from_store(profile, region, run):
    """Parameter Store 에서 앱키와 시크릿만 읽는다. 다른 값은 환경에 올리지 않는다."""
    parameters = lc.fetch_parameters(profile, region, run, lc.COLLECTOR_PREFIX)
    updates, _ = lc.to_env_updates(parameters, lc.COLLECTOR_PREFIX, lc.COLLECTOR_ENV_NAMES)
    wanted = {name: updates[name] for name in ("BROKER_APP_KEY", "BROKER_APP_SECRET") if name in updates}
    if len(wanted) != 2:
        raise RuntimeError("Parameter Store 에서 앱키와 시크릿을 모두 읽지 못했습니다")
    return wanted


def build_command(root, test_class=TEST_CLASS):
    return [os.path.join(root, "gradlew"), "-p", root, ":services:collector:cleanTest", ":services:collector:test",
            "--tests", f"*{test_class}", "--no-build-cache"]


def build_env(base_env, credentials, args, out_path):
    """시험 프로세스의 환경변수. 키는 여기로만 넘어가고 명령줄에는 들어가지 않는다."""
    env = dict(base_env)
    env.update(credentials)
    env.update({
        GUARD_ENV: "1",
        "BROKER_RATE_PROBE": "1",
        "BROKER_BASE_URL": args.base_url,
        "BROKER_PROBE_SYMBOLS": args.symbols,
        "BROKER_PROBE_OUT": out_path,
        "BROKER_RATE_PROBE_OK_CODE": args.ok_code,
        "BROKER_RATE_PROBE_RATES": args.rates,
        "BROKER_RATE_PROBE_PHASE_SECONDS": str(args.phase_seconds),
    })
    if args.probe == "smoke":
        env["BROKER_SMOKE"] = "1"
    return env


def main(argv=None, run=subprocess.run, now=None, base_env=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", help="AWS CLI 프로필. 없으면 기본 자격증명 방식을 쓴다")
    parser.add_argument("--region", default=lc.DEFAULT_REGION)
    parser.add_argument("--base-url", default=os.environ.get("BROKER_BASE_URL", ""), help="시세 제공자 운영 주소")
    parser.add_argument("--symbols", required=True, help="시험할 종목(쉼표 구분, 첫 종목만 씀). 저장소에 적지 않는다")
    parser.add_argument("--probe", choices=["rate", "smoke"], default="rate",
                        help="rate: 호출 속도 시험, smoke: 합쳐진 BrokerClient 연동 확인(토큰 1회, 호출 3회)")
    parser.add_argument("--ok-code", default="", help="정상 응답의 processCode(rate 에서 필요)")
    parser.add_argument("--rates", default="1,2,3", help="초당 호출 수(쉼표 구분)")
    parser.add_argument("--phase-seconds", type=int, default=30, help="단계 시간(초)")
    parser.add_argument("--out", help="결과 파일. 기본은 data/ 아래")
    parser.add_argument("--cooldown-minutes", type=int, default=DEFAULT_COOLDOWN_MINUTES)
    parser.add_argument("--force", action="store_true", help="쉬는 시간을 넘긴다(토큰 거절 표식은 넘지 못함)")
    parser.add_argument("--root", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = parser.parse_args(argv)

    if not args.base_url:
        print("중단: --base-url 또는 BROKER_BASE_URL 이 필요합니다")
        return 2
    if args.probe == "rate" and not args.ok_code:
        print("중단: rate 시험에는 --ok-code 가 필요합니다")
        return 2
    env_source = os.environ if base_env is None else base_env
    if not env_source.get("JAVA_HOME_21"):
        print("중단: JAVA_HOME_21 이 필요합니다")
        return 2

    injected_now = now
    now = now or datetime.now(timezone.utc)
    marker_path = os.path.join(args.root, "data", MARKER_NAME)
    reason = refusal(read_marker(marker_path), now, args.cooldown_minutes, args.force)
    if reason:
        print("중단:", reason)
        return 3

    try:
        credentials = credentials_from_store(args.profile, args.region, run)
    except (RuntimeError, ValueError) as error:
        print("중단:", error)
        return 1

    out_path = args.out or os.path.join(args.root, "data", f"{args.probe}-probe-{now.strftime('%Y%m%d-%H%M%S')}.txt")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    test_class = SMOKE_TEST_CLASS if args.probe == "smoke" else TEST_CLASS
    result = run(build_command(args.root, test_class), env=build_env(env_source, credentials, args, out_path),
                 capture_output=True, text=True)

    output = open(out_path, encoding="utf-8").read() if os.path.exists(out_path) else ""
    outcome = classify_smoke(output) if args.probe == "smoke" else classify(output)
    # 쉬는 시간은 호출이 끝난 시각부터 센다. 몇 분짜리 시험의 시작 시각으로 적으면 쉬는 시간이 그만큼 줄어든다.
    write_marker(marker_path, outcome, injected_now or datetime.now(timezone.utc))
    print(output.rstrip() or "(결과 파일이 비어 있습니다)")
    print("끝난 모양:", OUTCOME_TEXT[outcome])
    return 0 if outcome == OUTCOME_OK and result.returncode == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
