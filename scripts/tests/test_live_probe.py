import json
import os
import subprocess
from datetime import datetime, timedelta, timezone

import load_config as lc
import live_probe as lp

NOW = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
KEY_VALUE = "fake-app-key-value"
SECRET_VALUE = "fake-app-secret-value"
OK_OUTPUT = "token: http=200 processCode=0000 expires_in=1\nphase 5/s: 계획=10 전송=10 응답=10 정상=10 평균=5ms 최대=9ms\n"
LIMIT_OUTPUT = OK_OUTPUT + "FIRST ABNORMAL: http=500 19ms header={}\nRESULT: 이상 응답으로 중단\n"
REJECTED_OUTPUT = "token: http=500 processCode=E000 expires_in=0\n"


def store_parameters():
    # 제공자 이름이 시험 코드에 새로 드러나지 않게, 기존 매핑에서 파라미터 이름을 거꾸로 찾는다.
    by_env = {env: short for short, env in lc.COLLECTOR_ENV_NAMES.items()}
    return [{"Name": lc.COLLECTOR_PREFIX + by_env["BROKER_APP_KEY"], "Value": KEY_VALUE},
            {"Name": lc.COLLECTOR_PREFIX + by_env["BROKER_APP_SECRET"], "Value": SECRET_VALUE},
            {"Name": lc.COLLECTOR_PREFIX + "dart-api-key", "Value": "unrelated-value"}]


class FakeRunner:
    """aws 호출과 gradle 호출을 흉내 낸다. gradle 이 결과 파일을 쓰는 것까지 흉내 낸다."""

    def __init__(self, gradle_output, store=None, aws_returncode=0):
        self.gradle_output = gradle_output
        self.store = store_parameters() if store is None else store
        self.aws_returncode = aws_returncode
        self.gradle_calls = []

    def __call__(self, cmd, **kwargs):
        if cmd[0] == "aws":
            return subprocess.CompletedProcess(cmd, self.aws_returncode, json.dumps({"Parameters": self.store}), "")
        self.gradle_calls.append((cmd, kwargs["env"]))
        with open(kwargs["env"]["BROKER_PROBE_OUT"], "w", encoding="utf-8") as f:
            f.write(self.gradle_output)
        return subprocess.CompletedProcess(cmd, 0, "", "")


def run_main(tmp_path, runner, extra=(), now=NOW):
    argv = ["--root", str(tmp_path), "--base-url", "https://example.invalid", "--symbols", "TEST01",
            "--ok-code", "0000", "--rates", "5", "--phase-seconds", "2", *extra]
    return lp.main(argv, run=runner, now=now, base_env={"JAVA_HOME_21": "/jdk"})


def marker(tmp_path):
    return json.loads((tmp_path / "data" / lp.MARKER_NAME).read_text())


def test_classify_tells_the_three_endings_apart():
    assert lp.classify(OK_OUTPUT) == lp.OUTCOME_OK
    assert lp.classify(LIMIT_OUTPUT) == lp.OUTCOME_ABNORMAL
    assert lp.classify(REJECTED_OUTPUT) == lp.OUTCOME_TOKEN_REJECTED
    assert lp.classify("") == lp.OUTCOME_TOKEN_REJECTED


def test_credentials_come_from_the_store_and_only_the_two_broker_values():
    credentials = lp.credentials_from_store(None, "r", FakeRunner(""))
    assert credentials == {"BROKER_APP_KEY": KEY_VALUE, "BROKER_APP_SECRET": SECRET_VALUE}


def test_missing_credentials_in_the_store_stop_before_calling_gradle(tmp_path, capsys):
    runner = FakeRunner(OK_OUTPUT, store=[])
    assert run_main(tmp_path, runner) == 1
    assert runner.gradle_calls == []
    assert "읽지 못했습니다" in capsys.readouterr().out


def test_store_read_failure_stops_before_calling_gradle(tmp_path):
    runner = FakeRunner(OK_OUTPUT, aws_returncode=1)
    assert run_main(tmp_path, runner) == 1
    assert runner.gradle_calls == []


def test_secrets_reach_the_test_only_through_the_environment_and_never_the_command_or_output(tmp_path, capsys):
    runner = FakeRunner(OK_OUTPUT)
    assert run_main(tmp_path, runner) == 0

    command, env = runner.gradle_calls[0]
    assert KEY_VALUE not in " ".join(command) and SECRET_VALUE not in " ".join(command)
    assert env["BROKER_APP_KEY"] == KEY_VALUE and env["BROKER_APP_SECRET"] == SECRET_VALUE
    out = capsys.readouterr().out
    assert KEY_VALUE not in out and SECRET_VALUE not in out
    assert "unrelated-value" not in " ".join(env.values())


def test_guard_and_settings_are_passed_to_the_probe(tmp_path):
    runner = FakeRunner(OK_OUTPUT)
    run_main(tmp_path, runner)

    _, env = runner.gradle_calls[0]
    assert env[lp.GUARD_ENV] == "1" and env["BROKER_RATE_PROBE"] == "1"
    assert env["BROKER_PROBE_SYMBOLS"] == "TEST01" and env["BROKER_RATE_PROBE_RATES"] == "5"
    assert env["BROKER_RATE_PROBE_PHASE_SECONDS"] == "2"
    command, _ = runner.gradle_calls[0]
    assert "*BrokerRateProbeTest" in command and "--no-build-cache" in command


def test_a_rejected_token_blocks_every_later_run_even_with_force(tmp_path, capsys):
    assert run_main(tmp_path, FakeRunner(REJECTED_OUTPUT)) == 1
    assert marker(tmp_path)["outcome"] == lp.OUTCOME_TOKEN_REJECTED

    again = FakeRunner(OK_OUTPUT)
    assert run_main(tmp_path, again, ["--force"], NOW + timedelta(days=1)) == 3
    assert again.gradle_calls == []
    assert "토큰 발급이 거절" in capsys.readouterr().out


def test_deleting_the_marker_allows_a_run_after_a_rejected_token(tmp_path):
    run_main(tmp_path, FakeRunner(REJECTED_OUTPUT))
    (tmp_path / "data" / lp.MARKER_NAME).unlink()

    assert run_main(tmp_path, FakeRunner(OK_OUTPUT)) == 0


def test_an_abnormal_stop_enforces_a_cooldown_that_force_can_skip(tmp_path, capsys):
    assert run_main(tmp_path, FakeRunner(LIMIT_OUTPUT)) == 1
    assert marker(tmp_path)["outcome"] == lp.OUTCOME_ABNORMAL

    too_soon = FakeRunner(OK_OUTPUT)
    assert run_main(tmp_path, too_soon, now=NOW + timedelta(minutes=3)) == 3
    assert too_soon.gradle_calls == []
    assert "분 뒤" in capsys.readouterr().out

    assert run_main(tmp_path, FakeRunner(OK_OUTPUT), ["--force"], NOW + timedelta(minutes=3)) == 0


def test_after_the_cooldown_a_run_is_allowed_again(tmp_path):
    run_main(tmp_path, FakeRunner(LIMIT_OUTPUT))

    assert run_main(tmp_path, FakeRunner(OK_OUTPUT), now=NOW + timedelta(minutes=11)) == 0
    assert marker(tmp_path)["outcome"] == lp.OUTCOME_OK


def test_the_marker_is_stamped_with_the_finish_time_not_the_start_time(tmp_path):
    before = datetime.now(timezone.utc)
    lp.main(["--root", str(tmp_path), "--base-url", "https://example.invalid", "--symbols", "TEST01",
             "--ok-code", "0000"], run=FakeRunner(LIMIT_OUTPUT), base_env={"JAVA_HOME_21": "/jdk"})

    assert datetime.fromisoformat(marker(tmp_path)["at"]) >= before


def test_missing_java_home_or_base_url_stops_early(tmp_path, capsys):
    runner = FakeRunner(OK_OUTPUT)
    argv = ["--root", str(tmp_path), "--symbols", "TEST01", "--ok-code", "0000"]

    assert lp.main(argv, run=runner, now=NOW, base_env={"JAVA_HOME_21": "/jdk"}) == 2
    assert lp.main(argv + ["--base-url", "https://example.invalid"], run=runner, now=NOW, base_env={}) == 2
    assert runner.gradle_calls == []
