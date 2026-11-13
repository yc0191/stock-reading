"""수동 실행 명령 스크립트(collector-command.sh)를 가짜 docker 로 돌린다. 실제 Kafka 에는 보내지 않는다."""
import json
import os
import re
import stat
import subprocess
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "collector-command.sh"
SCHEMA = json.loads((REPO_ROOT / "contracts" / "events" / "collector.command.v1.schema.json").read_text(encoding="utf-8"))
COMMANDS = ["collect", "collect-news", "collect-pre-open", "collect-intraday", "collect-post-close", "collect-etf-members"]
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


def run(tmp_path: Path, *args: str, docker_output: str = "", docker_exit: int = 0
        ) -> tuple[subprocess.CompletedProcess, list[str], str]:
    """가짜 docker 는 받은 인자와 표준입력을 기록하고, 주어진 출력과 종료 코드를 낸다."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    args_log, stdin_log = tmp_path / "args.log", tmp_path / "stdin.log"
    for f in (args_log, stdin_log):
        f.unlink(missing_ok=True)
    docker = bindir / "docker"
    docker.write_text(f'#!/bin/sh\nfor a in "$@"; do echo "$a" >> "{args_log}"; done\ncat > "{stdin_log}"\n'
                      f'printf \'%s\' "{docker_output}" >&2\nexit {docker_exit}\n')
    docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}",
               KAFKA_CONTAINER="test-kafka", KAFKA_BOOTSTRAP="localhost:59998")
    result = subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=30)
    sent_args = args_log.read_text().splitlines() if args_log.exists() else []
    stdin = stdin_log.read_text() if stdin_log.exists() else ""
    return result, sent_args, stdin


@pytest.mark.parametrize("command", COMMANDS)
def test_each_command_sends_one_schema_valid_event_to_the_command_topic(tmp_path, command):
    result, args, stdin = run(tmp_path, command)

    assert result.returncode == 0, result.stderr
    assert args[:3] == ["exec", "-i", "test-kafka"]
    assert args[args.index("--bootstrap-server") + 1] == "localhost:59998"
    assert args[args.index("--topic") + 1] == "collector.command"
    lines = stdin.splitlines()
    assert len(lines) == 1
    key, value = lines[0].split("|", 1)
    assert key == command
    event = json.loads(value)
    checker = FormatChecker()
    errors = [e.message for e in Draft202012Validator(SCHEMA, format_checker=checker).iter_errors(event)]
    assert errors == []
    # jsonschema 는 선택 패키지가 없으면 date-time 을 검사하지 않으므로 직접 본다.
    assert RFC3339.match(event["occurredAt"]) and RFC3339.match(event["producedAt"])
    assert event["payload"]["command"] == command
    assert event["eventId"] in result.stdout


def test_each_run_has_a_new_event_id(tmp_path):
    # 수집기는 같은 eventId 를 한 번만 실행하므로, 같은 명령을 다시 보내면 새 eventId 여야 다시 돈다.
    ids = [json.loads(run(tmp_path, "collect")[2].split("|", 1)[1])["eventId"] for _ in range(2)]
    assert ids[0] != ids[1]


@pytest.mark.parametrize("args", [[], ["collect", "collect-news"], ["collect-members"], ["daily-briefing-morning"],
                                  ['collect"}']])
def test_missing_extra_or_unknown_commands_are_refused_without_sending(tmp_path, args):
    result, sent, stdin = run(tmp_path, *args)

    assert result.returncode == 2
    assert "사용법" in result.stderr
    assert sent == [] and stdin == ""


# 콘솔 프로듀서는 토픽이 없거나 시간이 지나 보내지 못해도 ERROR 줄만 찍고 0으로 끝난다(로컬에서 실제로 확인한 출력의 앞부분).
SEND_ERROR = ("ERROR Error when sending message to topic collector.command with key: 7 bytes, value: 280 bytes with error: "
              "org.apache.kafka.common.errors.TimeoutException: Topic collector.command not present in metadata after 60000 ms.")


@pytest.mark.parametrize("output, code", [(SEND_ERROR, 0), ("", 1)])
def test_a_failed_send_is_reported_as_failure_not_as_sent(tmp_path, output, code):
    result, _, _ = run(tmp_path, "collect", docker_output=output, docker_exit=code)

    assert result.returncode == 1
    assert "보냈습니다" not in result.stdout
    assert "보내지 못했습니다" in result.stderr


def test_a_send_with_only_warnings_is_still_reported_as_sent(tmp_path):
    # 새 토픽에 처음 보낼 때 콘솔 프로듀서는 성공하면서도 소문자 Error 가 든 경고를 찍는다. 이것을 실패로 보지 않는다.
    warning = "WARN [Producer clientId=console-producer] Error while fetching metadata with correlation id 1 : {collector.command=LEADER_NOT_AVAILABLE}"
    result, _, _ = run(tmp_path, "collect", docker_output=warning, docker_exit=0)

    assert result.returncode == 0, result.stderr
    assert "보냈습니다" in result.stdout
