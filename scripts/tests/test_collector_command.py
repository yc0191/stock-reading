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
COMMANDS = ["collect", "collect-news", "collect-pre-open", "collect-intraday", "collect-post-close"]
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


def run(tmp_path: Path, *args: str) -> tuple[subprocess.CompletedProcess, list[str], str]:
    """가짜 docker 는 받은 인자와 표준입력을 기록만 한다."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    args_log, stdin_log = tmp_path / "args.log", tmp_path / "stdin.log"
    for f in (args_log, stdin_log):
        f.unlink(missing_ok=True)
    docker = bindir / "docker"
    docker.write_text(f'#!/bin/sh\nfor a in "$@"; do echo "$a" >> "{args_log}"; done\ncat > "{stdin_log}"\n')
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
