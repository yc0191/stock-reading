"""글꼴 내려받기(install_font.py)를 file:// 주소로 돌려 SHA-256 검사와 이미 있는 파일 건너뛰기를 확인한다. 네트워크는 쓰지 않는다."""
import hashlib

import install_font

DATA = b"fake-font-bytes"
GOOD = hashlib.sha256(DATA).hexdigest()


def source(tmp_path, content=DATA):
    src = tmp_path / "src"
    src.mkdir()
    (src / "Fake-Regular.otf").write_bytes(content)
    return src.as_uri()


def test_the_pinned_font_addresses_name_a_commit_and_every_font_has_a_sha256():
    assert install_font.COMMIT in install_font.BASE_URL and "/main/" not in install_font.BASE_URL
    assert install_font.BASE_URL.startswith("https://")
    assert all(len(sha) == 64 and int(sha, 16) >= 0 for sha in install_font.FONTS.values())


def test_a_font_with_the_expected_sha256_is_saved(tmp_path, capsys):
    target = tmp_path / "fonts"
    assert install_font.install(target, source(tmp_path), {"Fake-Regular.otf": GOOD}) == 0
    assert (target / "Fake-Regular.otf").read_bytes() == DATA
    assert "저장" in capsys.readouterr().out


def test_a_font_with_a_different_sha256_is_not_saved(tmp_path, capsys):
    target = tmp_path / "fonts"
    assert install_font.install(target, source(tmp_path, b"tampered"), {"Fake-Regular.otf": GOOD}) == 1
    assert not (target / "Fake-Regular.otf").exists()
    assert "SHA-256" in capsys.readouterr().err


def test_a_font_that_is_already_right_is_not_downloaded_again(tmp_path, capsys):
    target = tmp_path / "fonts"
    target.mkdir()
    (target / "Fake-Regular.otf").write_bytes(DATA)
    # 주소가 없어도 이미 맞는 파일이 있으면 받으러 가지 않는다.
    assert install_font.install(target, (tmp_path / "nowhere").as_uri(), {"Fake-Regular.otf": GOOD}) == 0
    assert "이미 있음" in capsys.readouterr().out
