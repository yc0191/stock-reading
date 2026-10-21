import json
import re
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts" / "events"

# jsonschema는 선택 패키지가 설치돼 있을 때만 "date-time"을 검사하고, 없으면 조용히
# 아무 값이나 통과시킨다. 잘못된 시각이 실제로 거부되도록 RFC 3339 검사를 직접 등록한다.
_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
_format_checker = FormatChecker()


@_format_checker.checks("date-time")
def _is_rfc3339(value) -> bool:
    return not isinstance(value, str) or bool(_RFC3339.match(value))


def validate(schema_file: str, instance: dict) -> list[str]:
    schema = json.loads((CONTRACTS_DIR / schema_file).read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=_format_checker)
    return [error.message for error in validator.iter_errors(instance)]


def sample(sample_file: str) -> dict:
    return json.loads((CONTRACTS_DIR / "samples" / "v1" / sample_file).read_text(encoding="utf-8"))


def sample_text(sample_file: str) -> str:
    return (CONTRACTS_DIR / "samples" / "v1" / sample_file).read_text(encoding="utf-8")
