"""테스트 지도(docs/test-map/)가 저장소의 본 코드·테스트와 맞는지 검사한다. 표준 라이브러리만 쓴다.

실행: python3 scripts/check_test_map.py [--root 저장소 루트]
문제가 없으면 종료 코드 0, 고칠 곳이 있으면 고칠 내용을 출력하고 1로 끝난다.
지도의 형식과 고치는 방법은 docs/test-map/README.md 에 있다.
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

MAP_DIR = Path("docs") / "test-map"
JAVA_SERVICES = ("collector", "market", "news", "notifier")
MAP_FILES = tuple(f"{name}.md" for name in (*JAVA_SERVICES, "briefing", "scripts"))
COLUMNS = ("기능", "대상", "단위", "가상", "실제")
TEST_COLUMNS = ("단위", "가상", "실제")
MIN_ROWS, MAX_ROWS = 3, 10
# 칸 끝의 공백이 잘려도 이유가 빈 '없음 -' 을 형식 오류가 아닌 빈 이유로 알린다.
NONE_MARKER = re.compile(r"(?:해당 )?없음 -(?: |$)")
HELPER_PREFIX = "테스트 도우미:"
README_HINT = "형식은 docs/test-map/README.md"

# 들여쓰지 않은 선언만 최상위 타입으로 본다(중첩 타입은 들여쓰기가 있다).
JAVA_TOP_LEVEL = re.compile(
    r"^(?:@\w+(?:\([^)]*\))?\s+)*"
    r"(?:(?:public|protected|private|abstract|final|static|sealed|non-sealed|strictfp)\s+)*"
    r"(?:class|interface|enum|record|@interface)\s+([A-Za-z_]\w*)"
)
TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_.\-]*")
BRACKET = re.compile(r"\[[^\]]*\]")
PY_TEST_FILE = re.compile(r"test_\w+(?:\.py)?")


@dataclass(frozen=True)
class Row:
    line: int
    cells: dict


@dataclass(frozen=True)
class Inventory:
    main: dict  # 이름 -> 저장소 기준 경로
    tests: dict


def _rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def java_types(src: Path, root: Path) -> dict:
    found = {}
    for f in sorted(src.rglob("*.java")) if src.is_dir() else []:
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            m = JAVA_TOP_LEVEL.match(line)
            if m:
                found.setdefault(m.group(1), _rel(f, root))
    return found


def files(src: Path, root: Path, patterns: tuple, recursive: bool) -> dict:
    if not src.is_dir():
        return {}
    found = {}
    for pattern in patterns:
        for f in sorted(src.rglob(pattern) if recursive else src.glob(pattern)):
            if f.is_file() and "__pycache__" not in f.parts:
                found[f.name] = _rel(f, root)
    return found


def inventory(root: Path, map_file: str) -> Inventory:
    service = map_file[:-3]
    if service in JAVA_SERVICES:
        base = root / "services" / service / "src"
        return Inventory(java_types(base / "main" / "java", root), java_types(base / "test" / "java", root))
    if service == "briefing":
        base = root / "services" / "briefing"
        return Inventory(files(base / "src" / "briefing", root, ("*.py",), True),
                         files(base / "tests", root, ("*.py",), True))
    return Inventory(files(root / "scripts", root, ("*.py", "*.sh"), False),
                     files(root / "scripts" / "tests", root, ("*.py",), True))


def names_of(name: str) -> set:
    """모듈·셸 파일은 확장자가 있는 이름과 없는 이름을 같은 것으로 본다."""
    if name.endswith((".py", ".sh")):
        return {name, name.rsplit(".", 1)[0]}
    return {name}


def lookup(token: str, known: dict):
    for name in known:
        if token in names_of(name):
            return name
    return None


def split_row(line: str) -> list:
    return [c.strip().replace("`", "") for c in line.strip().strip("|").split("|")]


def parse_map(path: Path, errors: list, shown: str):
    """첫 표의 행과 표 뒤 '테스트 도우미' 줄의 이름 목록을 돌려준다. 도우미 줄이 없으면 None."""
    lines = path.read_text(encoding="utf-8").splitlines()
    header_at = None
    for i, line in enumerate(lines):
        if line.strip().startswith("|") and all(col in split_row(line) for col in COLUMNS):
            header_at = i
            break
    if header_at is None:
        errors.append(f"{shown}: 기능/대상/단위/가상/실제 칸이 있는 표가 없습니다. {README_HINT}의 '표의 칸'대로 표를 만드세요")
        return [], None
    header = split_row(lines[header_at])
    index = {col: header.index(col) for col in COLUMNS}
    rows = []
    end = header_at
    for j in range(header_at + 1, len(lines)):
        if not lines[j].strip().startswith("|"):
            break
        end = j
        cells = split_row(lines[j])
        if all(re.fullmatch(r":?-{3,}:?", c) for c in cells):
            continue
        if len(cells) < len(header):
            errors.append(f"{shown}:{j + 1}: 칸이 {len(cells)}개입니다. 표 머리와 같은 {len(header)}칸으로 맞추세요")
            continue
        rows.append(Row(j + 1, {col: cells[index[col]] for col in COLUMNS}))
    helper = None
    for line in lines[end + 1:]:
        text = line.strip().replace("`", "")
        if text.startswith(HELPER_PREFIX):
            rest = text[len(HELPER_PREFIX):].strip()
            helper = [] if rest in ("", "없음") else [n.strip() for n in rest.split(",") if n.strip()]
            break
    return rows, helper


def check_cell(cell: str, where: str, col: str, tests: dict, java: bool, errors: list) -> set:
    """칸 하나를 검사하고 칸에 적힌 테스트 이름(인벤토리 이름)을 돌려준다."""
    linked = set()
    if not cell.strip():
        errors.append(f"{where}: '{col}' 칸이 비었습니다. 테스트 이름이나 '없음 - 이유', '해당 없음 - 이유'를 쓰세요")
        return linked
    for item in (part.strip() for part in cell.split(";")):
        if not item:
            continue
        if item.startswith(("없음", "해당 없음")):
            marker = NONE_MARKER.match(item)
            if marker is None:
                errors.append(f"{where}: '{col}' 칸의 '{item}' 은 '없음 - 이유' 또는 '해당 없음 - 이유' 형식이 아닙니다. "
                              f"' - ' 뒤에 이유를 쓰세요")
                continue
            reason = BRACKET.sub("", item[marker.end():]).strip()
            if len(reason) < 2:
                errors.append(f"{where}: '{col}' 칸의 '{item}' 에 이유가 없습니다. ' - ' 뒤에 왜 없는지 쓰세요")
            continue
        refs = []
        for token in TOKEN.findall(BRACKET.sub("", item)):
            name = lookup(token, tests)
            if name:
                refs.append(name)
            elif (java and token.endswith("Test")) or (not java and PY_TEST_FILE.fullmatch(token)):
                errors.append(f"{where}: '{col}' 칸의 테스트 {token} 가 저장소에 없습니다. "
                              f"이름을 실제 테스트에 맞게 고치거나, 지운 테스트면 칸에서 빼고 비면 '없음 - 이유'를 쓰세요")
                refs.append(token)
        if not refs:
            errors.append(f"{where}: '{col}' 칸의 '{item}' 에 테스트 이름도 '없음 - 이유'도 없습니다. "
                          f"테스트 이름이나 '없음 - 이유', '해당 없음 - 이유'를 쓰세요")
        linked.update(r for r in refs if r in tests)
    return linked


def check_map_file(root: Path, map_file: str, errors: list) -> None:
    shown = (MAP_DIR / map_file).as_posix()
    path = root / MAP_DIR / map_file
    if not path.is_file():
        errors.append(f"{shown}: 지도 파일이 없습니다. {README_HINT}대로 만드세요")
        return
    inv = inventory(root, map_file)
    java = map_file[:-3] in JAVA_SERVICES
    rows, helper = parse_map(path, errors, shown)
    if rows and not (MIN_ROWS <= len(rows) <= MAX_ROWS):
        errors.append(f"{shown}: 기능 행이 {len(rows)}개입니다. 파일당 {MIN_ROWS}~{MAX_ROWS}행이 되게 행을 합치거나 나누세요")

    counts = {}
    linked = set()
    for row in rows:
        where = f"{shown}:{row.line}"
        if not row.cells["기능"]:
            errors.append(f"{where}: '기능' 칸이 비었습니다. 이 행이 지키는 기능을 쓰세요")
        targets = [t.strip() for t in row.cells["대상"].split(",") if t.strip()]
        if not targets:
            errors.append(f"{where}: '대상' 칸이 비었습니다. 이 기능을 이루는 본 코드 이름을 쓰세요")
        for target in targets:
            name = lookup(target, inv.main)
            if name is None:
                errors.append(f"{where}: '대상' 칸의 {target} 가 본 코드에 없습니다. 지우거나 바뀐 이름으로 고치세요")
            else:
                counts[name] = counts.get(name, 0) + 1
        for col in TEST_COLUMNS:
            linked |= check_cell(row.cells[col], where, col, inv.tests, java, errors)

    if helper is None:
        if rows:
            errors.append(f"{shown}: 표 뒤에 '{HELPER_PREFIX}' 줄이 없습니다. 도우미가 없어도 '{HELPER_PREFIX} 없음' 줄을 두세요")
    else:
        for name in helper:
            found = lookup(name, inv.tests)
            if found is None:
                errors.append(f"{shown}: '{HELPER_PREFIX}' 줄의 {name} 가 테스트 폴더에 없습니다. 줄에서 빼거나 이름을 고치세요")
            else:
                linked.add(found)

    for name, source in sorted(inv.main.items()):
        if counts.get(name, 0) == 0:
            errors.append(f"{shown}: 본 코드 {name}({source})가 어느 행의 '대상' 칸에도 없습니다. "
                          f"알맞은 기능 행의 '대상'에 넣거나 새 기능 행을 만드세요")
        elif counts[name] > 1:
            errors.append(f"{shown}: 본 코드 {name} 가 '대상' 칸에 {counts[name]}번 있습니다. 한 행에만 남기세요")
    for name, source in sorted(inv.tests.items()):
        if name not in linked:
            errors.append(f"{shown}: 테스트 {name}({source})가 어느 기능 칸에도, '{HELPER_PREFIX}' 줄에도 없습니다. "
                          f"지키는 기능 행의 단위·가상·실제 칸에 넣거나, 돕기만 하는 파일이면 도우미 줄에 넣으세요")


def check(root: Path) -> list:
    errors = []
    if not (root / MAP_DIR / "README.md").is_file():
        errors.append(f"{(MAP_DIR / 'README.md').as_posix()}: 지도 설명 파일이 없습니다")
    for map_file in MAP_FILES:
        check_map_file(root, map_file, errors)
    return errors


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="테스트 지도와 저장소를 비교한다")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent, help="저장소 루트")
    args = parser.parse_args(argv)
    errors = check(args.root.resolve())
    for error in errors:
        print(error)
    if errors:
        print(f"테스트 지도 검사 실패: {len(errors)}건")
        return 1
    print("테스트 지도 검사 통과")
    return 0


if __name__ == "__main__":
    sys.exit(main())
