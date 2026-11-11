"""tick 발행 스크립트(publish-tick.sh)가 보내기 실패를 성공으로 알리지 않는지 가짜 docker 로 본다. 실제 Kafka 에는 보내지 않는다."""
import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "publish-tick.sh"
# 콘솔 프로듀서는 토픽이 없거나 시간이 지나 보내지 못해도 ERROR 줄만 찍고 0으로 끝난다.
SEND_ERROR = ("ERROR Error when sending message to topic schedule.tick with key: 22 bytes, value: 300 bytes with error: "
              "org.apache.kafka.common.errors.TimeoutException: Topic schedule.tick not present in metadata after 60000 ms.")


def run(tmp_path: Path, output: str, code: int) -> subprocess.CompletedProcess:
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    docker = bindir / "docker"
    docker.write_text(f'#!/bin/sh\ncat > /dev/null\nprintf \'%s\' "{output}" >&2\nexit {code}\n')
    docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}", KAFKA_CONTAINER="test-kafka", KAFKA_BOOTSTRAP="localhost:59998")
    return subprocess.run(["bash", str(SCRIPT), "daily-briefing-morning"], env=env, capture_output=True, text=True, timeout=30)


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
    warning = "WARN [Producer clientId=console-producer] Error while fetching metadata with correlation id 1 : {schedule.tick=LEADER_NOT_AVAILABLE}"
    result = run(tmp_path, warning, 0)

    assert result.returncode == 0, result.stderr
    assert "발행했습니다" in result.stdout
