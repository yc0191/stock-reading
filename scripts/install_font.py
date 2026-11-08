"""데일리 메일 차트의 한글 글꼴(Noto Sans KR 보통·굵게)을 내려받아 services/briefing/fonts/ 에 둔다.

저장소에는 버전 고정 주소와 SHA-256만 두고 글꼴 파일은 git에 넣지 않는다(.gitignore). 내려받은 파일의 SHA-256이
다르면 저장하지 않고 실패한다. 이미 맞는 파일이 있으면 다시 받지 않는다. 표준 라이브러리만 쓴다.

사용: python3 scripts/install_font.py
"""
from __future__ import annotations

import hashlib
import sys
import urllib.request
from pathlib import Path

# notofonts/noto-cjk 의 Sans2.004 태그 커밋(SIL OFL 1.1)
COMMIT = "523d033d6cb47f4a80c58a35753646f5c3608a78"
BASE_URL = f"https://raw.githubusercontent.com/notofonts/noto-cjk/{COMMIT}/Sans/SubsetOTF/KR"
FONTS = {
    "NotoSansKR-Regular.otf": "69975a0ac8472717870aefeab0a4d52739308d90856b9955313b2ad5e0148d68",
    "NotoSansKR-Bold.otf": "5a6ceb287ed2fc6cfc6213144ebea68cbd94b20fc9eb873d8486493bf02d9bda",
}
FONT_DIR = Path(__file__).resolve().parent.parent / "services" / "briefing" / "fonts"
TIMEOUT_SECONDS = 120


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def install(font_dir: Path = FONT_DIR, base_url: str = BASE_URL, fonts: dict[str, str] = FONTS) -> int:
    font_dir.mkdir(parents=True, exist_ok=True)
    for name, expected in fonts.items():
        target = font_dir / name
        if target.is_file() and sha256(target.read_bytes()) == expected:
            print(f"이미 있음: {name}")
            continue
        with urllib.request.urlopen(f"{base_url}/{name}", timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 - 고정 https 주소
            data = response.read()
        if sha256(data) != expected:
            print(f"SHA-256이 다름, 저장하지 않음: {name}", file=sys.stderr)
            return 1
        target.write_bytes(data)
        print(f"저장: {name}")
    return 0


if __name__ == "__main__":
    sys.exit(install())
