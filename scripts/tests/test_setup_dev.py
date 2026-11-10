"""로컬 준비 스크립트(setup-dev.sh)를 임시 폴더의 빈 저장소 모양에서 돌린다. 가짜 JDK·가짜 파이썬을 써서 네트워크와 실제 설치는 쓰지 않는다."""
import os
import shutil
import stat
import subprocess
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent


def executable(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    shutil.copy(SCRIPTS / "setup-dev.sh", root / "scripts")
    shutil.copy(SCRIPTS / "install_font.py", root / "scripts")
    (root / "services" / "briefing").mkdir(parents=True)
    (root / "services" / "briefing" / "requirements.txt").write_text("")
    return root


def fake_jdk(tmp_path: Path, version: str) -> Path:
    home = tmp_path / f"jdk-{version}"
    executable(home / "bin" / "java", f"echo 'openjdk version \"{version}\" 2026-01-01' >&2\n")
    return home


def fake_python(tmp_path: Path, pip_exit: int = 0) -> Path:
    """'-m venv <폴더>'면 pip 호출을 기록하고 pip_exit로 끝나는 가짜 venv 파이썬을 만들고, install_font.py 는 성공으로 끝낸다."""
    log = tmp_path / "calls.log"
    return executable(tmp_path / "bin" / "python3", f"""echo "python3 $*" >> "{log}"
if [ "$1" = -m ] && [ "$2" = venv ]; then
  mkdir -p "$3/bin"
  printf '#!/bin/sh\\necho "venv $*" >> "{log}"\\nexit {pip_exit}\\n' > "$3/bin/python"
  chmod +x "$3/bin/python"
fi
exit 0
""")


def run(root: Path, *args: str, java_home: Path | None = None, python: Path | None = None):
    env = {k: v for k, v in os.environ.items() if k != "JAVA_HOME_21"}
    if java_home is not None:
        env["JAVA_HOME_21"] = str(java_home)
    if python is not None:
        env["SETUP_PYTHON"] = str(python)
    return subprocess.run(["bash", str(root / "scripts" / "setup-dev.sh"), *args], env=env,
                          capture_output=True, text=True, timeout=60)


def line(result, item: str) -> str:
    return next(x for x in result.stdout.splitlines() if x.startswith(f"- {item}:"))


def files(root: Path) -> set[Path]:
    return set(root.rglob("*"))


def test_check_on_an_empty_clone_reports_every_missing_part_and_creates_nothing(tmp_path):
    root = repo(tmp_path)
    before = files(root)
    result = run(root, "--check", java_home=fake_jdk(tmp_path, "21.0.5"))

    assert result.returncode == 1
    assert "안 됨" in line(result, ".env") and ".env.example" in line(result, ".env")
    assert "준비됨" in line(result, "JDK 21")
    assert "안 됨" in line(result, "브리핑 venv") and "안 됨" in line(result, "차트 글꼴")
    # 글꼴 확인에 쓰는 파이썬 캐시(__pycache__)도 남기지 않는다.
    assert files(root) == before


def test_a_broken_env_link_and_a_jdk_that_is_not_21_are_reported(tmp_path):
    root = repo(tmp_path)
    (root / ".env").symlink_to(tmp_path / "missing.env")
    result = run(root, "--check", java_home=fake_jdk(tmp_path, "17.0.9"))

    assert result.returncode == 1
    assert "링크가 가리키는 파일이 없습니다" in line(result, ".env")
    assert "안 됨" in line(result, "JDK 21")


def test_setup_creates_the_venv_installs_requirements_and_fetches_fonts(tmp_path):
    root = repo(tmp_path)
    (root / ".env").write_text("KEY=\n")
    result = run(root, java_home=fake_jdk(tmp_path, "21.0.5"), python=fake_python(tmp_path))

    assert result.returncode == 0, result.stdout + result.stderr
    calls = (tmp_path / "calls.log").read_text().splitlines()
    assert calls[0] == "python3 -m venv services/briefing/.venv"
    assert calls[1].startswith("venv -m pip install") and calls[1].endswith("services/briefing/requirements.txt")
    assert calls[2] == "python3 scripts/install_font.py"
    assert all("준비됨" in line(result, item) for item in (".env", "JDK 21", "브리핑 venv", "차트 글꼴"))


def test_a_failed_requirements_install_is_reported_and_fails_the_run(tmp_path):
    root = repo(tmp_path)
    (root / ".env").write_text("KEY=\n")
    result = run(root, java_home=fake_jdk(tmp_path, "21.0.5"), python=fake_python(tmp_path, pip_exit=1))

    assert result.returncode == 1
    assert "의존성 설치에 실패했습니다" in line(result, "브리핑 venv")
    assert "준비됨" in line(result, "차트 글꼴")


def test_an_unknown_option_stops_before_checking_anything(tmp_path):
    result = run(repo(tmp_path), "--bogus")
    assert result.returncode == 2 and "사용법" in result.stderr and result.stdout == ""
