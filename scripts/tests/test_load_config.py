import json
import os
import stat
import subprocess

import pytest

import load_config as lc


def param(short, value):
    return {"Name": lc.PREFIX + short, "Value": value}


def full_parameters():
    return [param(short, f"v-{short}") for short in lc.ENV_NAMES]


def fake_run(parameters, returncode=0):
    def run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, returncode, json.dumps({"Parameters": parameters}), "")
    return run


def capturing_run(parameters):
    calls = []

    def run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": parameters}), "")
    return run, calls


def test_profile_is_omitted_unless_given(tmp_path):
    run, calls = capturing_run(full_parameters())
    lc.main(["--env-file", str(tmp_path / ".env"), "--dry-run"], run=run)
    assert "--profile" not in calls[0]


def test_profile_is_passed_when_given(tmp_path):
    run, calls = capturing_run(full_parameters())
    lc.main(["--env-file", str(tmp_path / ".env"), "--dry-run", "--profile", "demo"], run=run)
    assert calls[0][calls[0].index("--profile") + 1] == "demo"


def test_all_parameters_map_to_env_names():
    updates, ignored = lc.to_env_updates(full_parameters())
    assert updates["SMTP_HOST"] == "v-smtp-host"
    assert updates["NOTIFIER_RECIPIENT"] == "v-recipient"
    assert set(updates) == set(lc.ENV_NAMES.values())
    assert ignored == []


def test_send_switch_is_never_loaded_even_if_present():
    updates, ignored = lc.to_env_updates([param("send-enabled", "true")])
    assert "NOTIFIER_SEND_ENABLED" not in updates
    assert ignored == ["send-enabled"]
    assert "NOTIFIER_SEND_ENABLED" not in lc.ENV_NAMES.values()


def test_max_per_hour_stays_a_code_default():
    # 한도는 application.yml 기본값을 쓰므로 파라미터로 덮어쓰지 않는다.
    updates, ignored = lc.to_env_updates([param("max-per-hour", "100")])
    assert "NOTIFIER_MAX_PER_HOUR" not in updates
    assert ignored == ["max-per-hour"]


def test_value_with_newline_is_rejected():
    with pytest.raises(ValueError):
        lc.to_env_updates([param("smtp-host", "a\nSMTP_PASSWORD=x")])


def test_missing_parameters_are_reported():
    updates, _ = lc.to_env_updates([param("smtp-host", "h")])
    assert "sender" in lc.missing_names(updates)
    assert "smtp-host" not in lc.missing_names(updates)


def test_merge_changes_only_target_keys():
    old = "POSTGRES_DB=x\n# 주석\nNOTIFIER_SEND_ENABLED=false\nSMTP_HOST=old\n"
    new = lc.merge_env(old, {"SMTP_HOST": "new", "SMTP_PORT": "587"})
    assert new == "POSTGRES_DB=x\n# 주석\nNOTIFIER_SEND_ENABLED=false\nSMTP_HOST=new\nSMTP_PORT=587\n"


def test_merge_ignores_commented_keys():
    new = lc.merge_env("# SMTP_HOST=old\n", {"SMTP_HOST": "new"})
    assert new == "# SMTP_HOST=old\nSMTP_HOST=new\n"


def test_main_writes_when_all_values_present(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("NOTIFIER_SEND_ENABLED=false\nSMTP_HOST=old\n")
    code = lc.main(["--env-file", str(env)], run=fake_run(full_parameters()))
    assert code == 0
    text = env.read_text()
    assert "SMTP_HOST=v-smtp-host" in text
    assert "NOTIFIER_SEND_ENABLED=false" in text
    assert stat.S_IMODE(os.stat(env).st_mode) == 0o600
    # 출력에는 키 이름만 있고 값은 없어야 한다.
    assert "v-smtp-password" not in capsys.readouterr().out


def test_main_writes_nothing_when_partial(tmp_path):
    env = tmp_path / ".env"
    env.write_text("SMTP_HOST=old\n")
    code = lc.main(["--env-file", str(env)], run=fake_run([param("smtp-host", "new")]))
    assert code == 1
    assert env.read_text() == "SMTP_HOST=old\n"


def test_dry_run_does_not_write(tmp_path):
    env = tmp_path / ".env"
    env.write_text("SMTP_HOST=old\n")
    assert lc.main(["--env-file", str(env), "--dry-run"], run=fake_run(full_parameters())) == 0
    assert env.read_text() == "SMTP_HOST=old\n"


def test_read_failure_does_not_echo_raw_error(tmp_path, capsys):
    def run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 255, "", "arn:aws:iam::123456789012:user/x denied")
    assert lc.main(["--env-file", str(tmp_path / ".env")], run=run) == 1
    assert "123456789012" not in capsys.readouterr().out


def collector_param(short, value):
    return {"Name": lc.COLLECTOR_PREFIX + short, "Value": value}


def test_every_collector_parameter_maps_to_its_env_name():
    parameters = [collector_param(short, f"v-{short}") for short in lc.COLLECTOR_ENV_NAMES]
    updates, ignored = lc.to_env_updates(parameters, lc.COLLECTOR_PREFIX, lc.COLLECTOR_ENV_NAMES)
    assert updates == {env: f"v-{short}" for short, env in lc.COLLECTOR_ENV_NAMES.items()}
    assert ignored == []


def test_news_search_settings_map_to_the_env_names_the_collector_reads():
    names = {
        "news-search-base-url": "NEWS_SEARCH_BASE_URL",
        "news-search-id-header": "NEWS_SEARCH_ID_HEADER",
        "news-search-secret-header": "NEWS_SEARCH_SECRET_HEADER",
        "news-search-queries": "NEWS_SEARCH_QUERIES",
    }
    parameters = [collector_param(short, f"v-{short}") for short in names]
    updates, ignored = lc.to_env_updates(parameters, lc.COLLECTOR_PREFIX, lc.COLLECTOR_ENV_NAMES)
    assert updates == {env: f"v-{short}" for short, env in names.items()}
    assert ignored == []


def test_queries_value_with_separators_is_written_as_is(tmp_path):
    # 검색어 설정은 '=', ',', ';' 를 쓰는 형식이라 값이 그대로 .env 에 들어가야 한다.
    env = tmp_path / ".env"
    queries = "테스트검색어=TEST01,TEST02;테스트검색어2=*"

    def run(cmd, **kwargs):
        params = full_parameters() if "/notifier" in cmd[cmd.index("--path") + 1] \
            else [collector_param("news-search-queries", queries)]
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": params}), "")

    assert lc.main(["--env-file", str(env)], run=run) == 0
    assert f"NEWS_SEARCH_QUERIES={queries}" in env.read_text()


def test_queries_value_with_newline_is_rejected():
    with pytest.raises(ValueError):
        lc.to_env_updates([collector_param("news-search-queries", "a=TEST01\nSMTP_PASSWORD=x")],
                          lc.COLLECTOR_PREFIX, lc.COLLECTOR_ENV_NAMES)


def test_main_loads_collector_key_after_notifier(tmp_path, capsys):
    env = tmp_path / ".env"
    calls = []

    def run(cmd, **kwargs):
        calls.append(cmd)
        params = full_parameters() if "/notifier" in cmd[cmd.index("--path") + 1] else [collector_param("dart-api-key", "secret-k")]
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": params}), "")

    assert lc.main(["--env-file", str(env)], run=run) == 0
    assert "DART_API_KEY=secret-k" in env.read_text()
    assert "secret-k" not in capsys.readouterr().out


def test_main_still_works_without_collector_key(tmp_path):
    env = tmp_path / ".env"

    def run(cmd, **kwargs):
        params = full_parameters() if "/notifier" in cmd[cmd.index("--path") + 1] else []
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": params}), "")

    assert lc.main(["--env-file", str(env)], run=run) == 0
    assert "DART_API_KEY" not in env.read_text()
