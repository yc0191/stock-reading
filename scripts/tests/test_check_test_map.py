from pathlib import Path

import check_test_map as ctm

REPO_ROOT = Path(__file__).resolve().parents[2]
HEADER = "| 기능 | 대상 | 단위 | 가상 | 실제 |\n|---|---|---|---|---|\n"


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def map_text(rows, helper="없음") -> str:
    body = "".join(f"| {' | '.join(cells)} |\n" for cells in rows)
    return f"# 지도\n\n{HEADER}{body}\n테스트 도우미: {helper}\n"


def java_rows(prefix: str):
    return [
        (f"기능 {prefix}A", f"{prefix}Alpha", f"{prefix}AlphaTest", "해당 없음 - 외부 의존 없음", "해당 없음 - 외부 의존 없음"),
        (f"기능 {prefix}B", f"{prefix}Beta, {prefix}BetaPort", f"{prefix}BetaTest", f"{prefix}BetaFlowTest",
         "없음 - 나중에 채움 [DB]"),
        (f"기능 {prefix}C", f"{prefix}Gamma", "없음 - 아직 없음", f"{prefix}GammaTest",
         f"{prefix}GammaLiveTest [외부 API, 켜는 조건: GAMMA_LIVE=1]; 없음 - 나중에 채움 [서비스 흐름]"),
    ]


def make_repo(root: Path) -> Path:
    """네 경우가 모두 없는 작은 가짜 저장소. 각 시험이 여기서 한 곳만 망가뜨린다."""
    write(root / "docs/test-map/README.md", "# 테스트 지도\n")
    for service in ctm.JAVA_SERVICES:
        prefix = service.capitalize()
        main = root / f"services/{service}/src/main/java/dev/x/{service}"
        test = root / f"services/{service}/src/test/java/dev/x/{service}"
        write(main / f"{prefix}Alpha.java", f"package dev.x;\n\npublic class {prefix}Alpha {{\n    record Inner() {{}}\n}}\n")
        write(main / f"sub/{prefix}Beta.java", f"package dev.x;\n\nrecord {prefix}Beta(int v) {{\n}}\n")
        write(main / f"sub/{prefix}BetaPort.java", f"package dev.x;\n\ninterface {prefix}BetaPort {{\n}}\n")
        write(main / f"{prefix}Gamma.java", f"package dev.x;\n\n@SuppressWarnings(\"x\")\nenum {prefix}Gamma {{ A }}\n")
        for name in ("AlphaTest", "BetaTest", "BetaFlowTest", "GammaTest", "GammaLiveTest"):
            write(test / f"{prefix}{name}.java", f"package dev.x;\n\nclass {prefix}{name} {{\n}}\n")
        write(test / f"{prefix}FakeServer.java", f"package dev.x;\n\nfinal class {prefix}FakeServer {{\n}}\n")
        write(root / f"docs/test-map/{service}.md", map_text(java_rows(prefix), helper=f"{prefix}FakeServer"))

    write(root / "services/briefing/src/briefing/__init__.py", "")
    for module in ("jobs", "report", "main"):
        write(root / f"services/briefing/src/briefing/{module}.py", "")
    for name in ("test_jobs.py", "test_report.py", "test_runner.py", "contracts.py"):
        write(root / f"services/briefing/tests/{name}", "")
    write(root / "docs/test-map/briefing.md", map_text([
        ("작업", "`__init__.py`, jobs.py", "test_jobs.py", "해당 없음 - 외부 의존 없음", "해당 없음 - 외부 의존 없음"),
        ("리포트", "report.py", "없음 - 가상에서 확인", "test_report.py", "없음 - 나중에 채움 [외부 API]"),
        ("실행부", "main", "해당 없음 - 통신뿐", "test_runner", "없음 - 나중에 채움 [서비스 흐름]"),
    ], helper="contracts.py"))

    for name in ("load_config.py", "tool.py", "run-me.sh"):
        write(root / f"scripts/{name}", "")
    write(root / "scripts/pytest.ini", "[pytest]\n")
    for name in ("test_load_config.py", "test_tool.py"):
        write(root / f"scripts/tests/{name}", "")
    write(root / "docs/test-map/scripts.md", map_text([
        ("설정 로더", "load_config.py", "test_load_config.py", "test_load_config.py", "없음 - 나중에 채움 [외부 API]"),
        ("도구", "tool.py", "test_tool.py", "해당 없음 - 파일만 읽음", "해당 없음 - 외부 의존 없음"),
        ("수동 도구", "run-me.sh", "없음 - 사람이 돌림", "없음 - 사람이 돌림", "해당 없음 - 도구 자체가 확인 수단"),
    ]))
    return root


def replace(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text, f"시험 준비 실패: {old!r} 가 {path} 에 없음"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def test_the_fake_repo_passes(tmp_path):
    assert ctm.check(make_repo(tmp_path)) == []


def test_current_repo_passes():
    assert ctm.check(REPO_ROOT) == []


def test_class_not_in_any_target_fails_and_names_the_map_file(tmp_path):
    root = make_repo(tmp_path)
    write(root / "services/market/src/main/java/dev/x/market/MarketNew.java", "package dev.x;\n\nclass MarketNew {\n}\n")

    errors = ctm.check(root)

    assert len(errors) == 1
    assert errors[0].startswith("docs/test-map/market.md:")
    assert "MarketNew" in errors[0] and "'대상'" in errors[0] and "새 기능 행" in errors[0]


def test_module_and_shell_file_not_in_any_target_fail(tmp_path):
    root = make_repo(tmp_path)
    write(root / "services/briefing/src/briefing/extra.py", "")
    write(root / "scripts/new-tool.sh", "")

    errors = ctm.check(root)

    assert len(errors) == 2
    assert any(e.startswith("docs/test-map/briefing.md:") and "extra.py" in e for e in errors)
    assert any(e.startswith("docs/test-map/scripts.md:") and "new-tool.sh" in e for e in errors)


def test_empty_cell_fails_with_the_line_and_column(tmp_path):
    root = make_repo(tmp_path)
    replace(root / "docs/test-map/news.md", "| 없음 - 아직 없음 |", "|  |")

    errors = ctm.check(root)

    assert len(errors) == 1
    assert errors[0].startswith("docs/test-map/news.md:7:")
    assert "'단위' 칸이 비었습니다" in errors[0] and "'없음 - 이유'" in errors[0]


def test_none_without_reason_or_text_without_test_name_fails(tmp_path):
    root = make_repo(tmp_path)
    replace(root / "docs/test-map/notifier.md", "없음 - 아직 없음", "없음 - ")
    replace(root / "docs/test-map/scripts.md", "해당 없음 - 파일만 읽음", "나중에")

    errors = ctm.check(root)

    assert len(errors) == 2
    assert any(e.startswith("docs/test-map/notifier.md:7:") and "이유가 없습니다" in e for e in errors)
    assert any(e.startswith("docs/test-map/scripts.md:6:") and "테스트 이름도 '없음 - 이유'도 없습니다" in e
               for e in errors)


def test_listed_test_that_does_not_exist_fails(tmp_path):
    root = make_repo(tmp_path)
    (root / "services/collector/src/test/java/dev/x/collector/CollectorGammaLiveTest.java").unlink()
    (root / "services/briefing/tests/test_report.py").unlink()

    errors = ctm.check(root)

    assert len(errors) == 2
    assert any(e.startswith("docs/test-map/collector.md:7:") and "CollectorGammaLiveTest 가 저장소에 없습니다" in e
               for e in errors)
    assert any(e.startswith("docs/test-map/briefing.md:6:") and "test_report.py 가 저장소에 없습니다" in e
               for e in errors)


def test_listed_helper_that_does_not_exist_fails(tmp_path):
    root = make_repo(tmp_path)
    replace(root / "docs/test-map/market.md", "테스트 도우미: MarketFakeServer", "테스트 도우미: MarketFakeServer, Gone")

    errors = ctm.check(root)

    assert len(errors) == 1
    assert errors[0].startswith("docs/test-map/market.md:") and "Gone" in errors[0]


def test_test_file_linked_to_no_feature_fails(tmp_path):
    root = make_repo(tmp_path)
    write(root / "services/notifier/src/test/java/dev/x/notifier/NotifierOrphanTest.java",
          "package dev.x;\n\nclass NotifierOrphanTest {\n}\n")
    write(root / "scripts/tests/conftest.py", "")

    errors = ctm.check(root)

    assert len(errors) == 2
    assert any(e.startswith("docs/test-map/notifier.md:") and "NotifierOrphanTest" in e and "도우미 줄" in e
               for e in errors)
    assert any(e.startswith("docs/test-map/scripts.md:") and "conftest.py" in e for e in errors)


def test_helper_removed_from_the_helper_line_counts_as_unlinked(tmp_path):
    root = make_repo(tmp_path)
    replace(root / "docs/test-map/collector.md", "테스트 도우미: CollectorFakeServer", "테스트 도우미: 없음")

    errors = ctm.check(root)

    assert len(errors) == 1
    assert "CollectorFakeServer" in errors[0] and "어느 기능 칸에도" in errors[0]


def test_duplicate_or_stale_target_fails(tmp_path):
    root = make_repo(tmp_path)
    replace(root / "docs/test-map/market.md", "| MarketGamma |", "| MarketGamma, MarketAlpha, MarketRemoved |")

    errors = ctm.check(root)

    assert len(errors) == 2
    assert any("MarketAlpha" in e and "2번" in e for e in errors)
    assert any("MarketRemoved" in e and "본 코드에 없습니다" in e for e in errors)


def test_missing_map_file_and_helper_line_and_row_count_are_reported(tmp_path):
    root = make_repo(tmp_path)
    (root / "docs/test-map/news.md").unlink()
    replace(root / "docs/test-map/scripts.md", "테스트 도우미: 없음\n", "")
    briefing = root / "docs/test-map/briefing.md"
    lines = briefing.read_text(encoding="utf-8").splitlines(keepends=True)
    briefing.write_text("".join(lines[:5] + lines[7:]), encoding="utf-8")  # 둘째·셋째 행을 지워 한 행만 남긴다

    errors = ctm.check(root)

    assert any(e.startswith("docs/test-map/news.md:") and "지도 파일이 없습니다" in e for e in errors)
    assert any(e.startswith("docs/test-map/scripts.md:") and "테스트 도우미:" in e for e in errors)
    assert any(e.startswith("docs/test-map/briefing.md:") and "행이 1개" in e for e in errors)


def test_main_returns_zero_on_pass_and_one_with_messages_on_failure(tmp_path, capsys):
    root = make_repo(tmp_path)
    assert ctm.main(["--root", str(root)]) == 0
    assert "통과" in capsys.readouterr().out

    write(root / "scripts/added.py", "")
    assert ctm.main(["--root", str(root)]) == 1
    out = capsys.readouterr().out
    assert "docs/test-map/scripts.md" in out and "added.py" in out and "실패: 1건" in out
