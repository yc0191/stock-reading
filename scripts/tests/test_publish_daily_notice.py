"""판 시작 알림 발행 스크립트(publish-daily-notice.sh)가 계약에 맞는 알림을 보내고, 보내기 실패를 성공으로 알리지 않는지 가짜 docker 로 본다.
실제 Kafka 에는 보내지 않는다."""
import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

from daily_e2e import schema_errors

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "publish-daily-notice.sh"
# 콘솔 프로듀서는 토픽이 없거나 시간이 지나 보내지 못해도 ERROR 줄만 찍고 0으로 끝난다.
SEND_ERROR = ("ERROR Error when sending message to topic daily.collection.completed with key: 7 bytes, value: 300 bytes with error: "
              "org.apache.kafka.common.errors.TimeoutException: Topic daily.collection.completed not present in metadata after 60000 ms.")


def run(tmp_path: Path, output: str, code: int, edition: str = "morning") -> subprocess.CompletedProcess:
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    docker = bindir / "docker"
    # 받은 "키|값" 한 줄을 파일로 남겨 보낸 알림을 검사한다.
    docker.write_text(f'#!/bin/sh\ncat > "{tmp_path}/sent"\nprintf \'%s\' "{output}" >&2\nexit {code}\n')
    docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}", KAFKA_CONTAINER="test-kafka", KAFKA_BOOTSTRAP="localhost:59998")
    return subprocess.run(["bash", str(SCRIPT), edition], env=env, capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize("edition", ["morning", "post-close"])
def test_the_sent_notice_passes_the_contract_and_is_keyed_by_the_edition(tmp_path, edition):
    result = run(tmp_path, "", 0, edition)

    assert result.returncode == 0, result.stderr
    key, value = (tmp_path / "sent").read_text().strip().split("|", 1)
    event = json.loads(value)
    assert key == edition and schema_errors("daily.collection.completed", event) == []
    assert event["payload"]["edition"] == edition and event["payload"]["missingSources"] == []


def test_an_unknown_edition_is_refused_without_sending(tmp_path):
    result = run(tmp_path, "", 0, "daily-briefing-morning")

    assert result.returncode == 2 and not (tmp_path / "sent").exists()


def test_a_successful_send_is_reported_as_sent(tmp_path):
    result = run(tmp_path, "", 0)

    assert result.returncode == 0, result.stderr
    assert "발행했습니다" in result.stdout


@pytest.mark.parametrize("output, code", [(SEND_ERROR, 0), ("", 1)])
def test_a_failed_send_is_reported_as_failure_not_as_sent(tmp_path, output, code):
    result = run(tmp_path, output, code)

    assert result.returncode == 1
    assert "발행했습니다" not in result.stdout
    assert "발행하지 못했습니다" in result.stderr


def test_a_send_with_only_warnings_is_still_reported_as_sent(tmp_path):
    # 새 토픽에 처음 보낼 때 콘솔 프로듀서는 성공하면서도 소문자 Error 가 든 경고를 찍는다. 이것을 실패로 보지 않는다.
    warning = "WARN [Producer clientId=console-producer] Error while fetching metadata with correlation id 1 : {daily.collection.completed=LEADER_NOT_AVAILABLE}"
    result = run(tmp_path, warning, 0)

    assert result.returncode == 0, result.stderr
    assert "발행했습니다" in result.stdout
