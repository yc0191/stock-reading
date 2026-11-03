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


def test_broker_url_and_live_symbol_map_to_the_env_names_test_all_reads():
    # test-all.sh --live 가 실행할 때 읽는 이름과 같아야 한다.
    names = {"broker-base-url": "BROKER_BASE_URL", "test-live-symbol": "TEST_ALL_LIVE_SYMBOL"}
    parameters = [collector_param(short, f"v-{short}") for short in names]
    updates, ignored = lc.to_env_updates(parameters, lc.COLLECTOR_PREFIX, lc.COLLECTOR_ENV_NAMES)
    assert updates == {env: f"v-{short}" for short, env in names.items()}
    assert ignored == []


def test_main_writes_live_symbol_without_printing_it(tmp_path, capsys):
    env = tmp_path / ".env"

    def run(cmd, **kwargs):
        params = full_parameters() if "/notifier" in cmd[cmd.index("--path") + 1] \
            else [collector_param("test-live-symbol", "TEST01"), collector_param("broker-base-url", "https://broker.invalid")]
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": params}), "")

    assert lc.main(["--env-file", str(env)], run=run) == 0
    text = env.read_text()
    assert "TEST_ALL_LIVE_SYMBOL=TEST01" in text
    assert "BROKER_BASE_URL=https://broker.invalid" in text
    out = capsys.readouterr().out
    assert "TEST01" not in out and "broker.invalid" not in out


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


def test_only_collector_skips_notifier_group(tmp_path, capsys):
    env = tmp_path / ".env"
    paths = []

    def run(cmd, **kwargs):
        path = cmd[cmd.index("--path") + 1]
        paths.append(path)
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": [collector_param("dart-api-key", "secret-k")]}), "")

    assert lc.main(["--env-file", str(env), "--only", "collector"], run=run) == 0
    assert paths == ["/stock-reading/dev/collector"]
    assert env.read_text() == "DART_API_KEY=secret-k\n"
    assert "SMTP" not in env.read_text()


def test_only_notifier_skips_collector_group(tmp_path):
    env = tmp_path / ".env"
    paths = []

    def run(cmd, **kwargs):
        paths.append(cmd[cmd.index("--path") + 1])
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": full_parameters()}), "")

    assert lc.main(["--env-file", str(env), "--only", "notifier"], run=run) == 0
    assert paths == ["/stock-reading/dev/notifier"]
    assert "SMTP_HOST=v-smtp-host" in env.read_text()


def broker_key_param(value):
    # 제공자 이름이 시험 코드에 새로 드러나지 않게, 기존 매핑에서 앱키 파라미터 이름을 거꾸로 찾는다.
    short = next(short for short, env in lc.COLLECTOR_ENV_NAMES.items() if env == "BROKER_APP_KEY")
    return collector_param(short, value)


def test_compare_reports_same_different_and_missing_without_returning_values():
    updates = {"BROKER_APP_KEY": "remote-value", "BROKER_APP_SECRET": "same-secret", "DART_API_KEY": "only-remote"}
    text = "BROKER_APP_KEY=remote-valueX\nBROKER_APP_SECRET=same-secret\n"

    rows = lc.compare_env(text, updates)

    assert rows == [
        ("BROKER_APP_KEY", "다름", len("remote-valueX"), len("remote-value")),
        ("BROKER_APP_SECRET", "같음", len("same-secret"), len("same-secret")),
        ("DART_API_KEY", "없음", None, len("only-remote")),
    ]


def test_unmapped_credential_names_flags_old_names_but_not_mapped_ones():
    updates = {"BROKER_APP_KEY": "v", "BROKER_APP_SECRET": "v"}
    text = "BROKER_APP_KEY=v\nOLD_APP_KEY=stale\nOLD_APP_SECRET=stale\nSMTP_HOST=h\n# COMMENTED_APP_KEY=x\n"

    assert lc.unmapped_credential_names(text, updates) == ["OLD_APP_KEY", "OLD_APP_SECRET"]


def test_check_prints_status_but_never_the_values_and_writes_nothing(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("BROKER_APP_KEY=stale-secret-value\n")
    before = env.read_text()

    code = lc.main(["--env-file", str(env), "--only", "collector", "--check"],
                   run=fake_run([broker_key_param("fresh-secret-value")]))

    out = capsys.readouterr().out
    assert code == 1
    assert "BROKER_APP_KEY: 다름" in out
    assert "stale-secret-value" not in out and "fresh-secret-value" not in out
    assert env.read_text() == before


def test_check_returns_zero_when_everything_matches(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("BROKER_APP_KEY=same-value\n")

    code = lc.main(["--env-file", str(env), "--only", "collector", "--check"],
                   run=fake_run([broker_key_param("same-value")]))

    assert code == 0
    assert "모두 같습니다" in capsys.readouterr().out


def test_check_fails_and_warns_when_an_unmanaged_old_name_is_present(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("BROKER_APP_KEY=same-value\nOLD_APP_KEY=stale\n")

    code = lc.main(["--env-file", str(env), "--only", "collector", "--check"],
                   run=fake_run([broker_key_param("same-value")]))

    out = capsys.readouterr().out
    assert code == 1
    assert "OLD_APP_KEY" in out and "stale" not in out


def briefing_param(short, value):
    return {"Name": lc.BRIEFING_PREFIX + short, "Value": value}


def group_run(paths, groups):
    """경로별로 다른 파라미터를 돌려주는 가짜 AWS CLI. 부른 경로를 paths에 남긴다."""
    def run(cmd, **kwargs):
        path = cmd[cmd.index("--path") + 1]
        paths.append(path)
        return subprocess.CompletedProcess(cmd, 0, json.dumps({"Parameters": groups.get(path, [])}), "")
    return run


def test_briefing_parameter_maps_to_the_env_name_the_briefing_reads():
    updates, ignored = lc.to_env_updates([briefing_param("anthropic-api-key", "v-key")],
                                         lc.BRIEFING_PREFIX, lc.BRIEFING_ENV_NAMES)
    assert updates == {"ANTHROPIC_API_KEY": "v-key"}
    assert ignored == []
    assert lc.BRIEFING_PREFIX == "/stock-reading/dev/briefing/"


def test_main_loads_briefing_key_without_printing_it(tmp_path, capsys):
    env = tmp_path / ".env"
    paths = []
    run = group_run(paths, {"/stock-reading/dev/notifier": full_parameters(),
                            "/stock-reading/dev/briefing": [briefing_param("anthropic-api-key", "secret-llm-key")]})

    assert lc.main(["--env-file", str(env)], run=run) == 0
    assert paths == ["/stock-reading/dev/notifier", "/stock-reading/dev/collector", "/stock-reading/dev/briefing"]
    assert "ANTHROPIC_API_KEY=secret-llm-key" in env.read_text()
    assert "secret-llm-key" not in capsys.readouterr().out


def test_main_still_works_without_briefing_key(tmp_path, capsys):
    env = tmp_path / ".env"
    run = group_run([], {"/stock-reading/dev/notifier": full_parameters()})

    assert lc.main(["--env-file", str(env)], run=run) == 0
    assert "ANTHROPIC_API_KEY" not in env.read_text()
    assert "briefing/anthropic-api-key 가 없어 ANTHROPIC_API_KEY 은 반영하지 않습니다" in capsys.readouterr().out


def test_only_briefing_skips_other_groups(tmp_path):
    env = tmp_path / ".env"
    paths = []
    run = group_run(paths, {"/stock-reading/dev/briefing": [briefing_param("anthropic-api-key", "secret-llm-key")]})

    assert lc.main(["--env-file", str(env), "--only", "briefing"], run=run) == 0
    assert paths == ["/stock-reading/dev/briefing"]
    assert env.read_text() == "ANTHROPIC_API_KEY=secret-llm-key\n"


def test_briefing_key_is_not_flagged_as_an_unmanaged_old_name():
    text = "ANTHROPIC_API_KEY=v\nBROKER_APP_KEY=v\n"
    assert lc.unmapped_credential_names(text, {"BROKER_APP_KEY": "v"}) == []


def test_check_with_matching_briefing_key_reports_no_mismatch_or_warning(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("ANTHROPIC_API_KEY=same-llm-key\n")

    code = lc.main(["--env-file", str(env), "--only", "briefing", "--check"],
                   run=fake_run([briefing_param("anthropic-api-key", "same-llm-key")]))

    out = capsys.readouterr().out
    assert code == 0
    assert "ANTHROPIC_API_KEY: 같음" in out
    assert "경고" not in out and "same-llm-key" not in out


def test_check_for_one_group_does_not_flag_keys_managed_by_another_group(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("ANTHROPIC_API_KEY=same-llm-key\nBROKER_APP_KEY=broker-key\nBROKER_APP_SECRET=broker-secret\n")

    code = lc.main(["--env-file", str(env), "--only", "briefing", "--check"],
                   run=fake_run([briefing_param("anthropic-api-key", "same-llm-key")]))

    out = capsys.readouterr().out
    assert code == 0
    assert "경고" not in out and "모두 같습니다" in out


def test_check_for_collector_does_not_warn_about_the_briefing_key(tmp_path, capsys):
    env = tmp_path / ".env"
    env.write_text("BROKER_APP_KEY=same-value\nANTHROPIC_API_KEY=other-llm-key\n")

    code = lc.main(["--env-file", str(env), "--only", "collector", "--check"],
                   run=fake_run([broker_key_param("same-value")]))

    out = capsys.readouterr().out
    assert code == 0
    assert "ANTHROPIC_API_KEY" not in out and "other-llm-key" not in out
