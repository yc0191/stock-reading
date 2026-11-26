"""시험용 가짜: 알림 메시지, 메모리 장부, 정해진 답을 주는 SQL 실행기, 메모리 결과 저장소."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ops_checker.metrics import MetricError
from ops_checker.results import UploadError
from ops_checker.store import ResultRow, RunRecord

KST = timezone(timedelta(hours=9))
EVENT_ID = "11111111-1111-4111-8111-111111111111"


def notification(edition="post-close", status="collected", event_id=EVENT_ID,
                 scheduled="2026-11-18T16:30:00+09:00") -> bytes:
    return json.dumps({
        "eventId": event_id, "eventType": "daily.collection.completed", "schemaVersion": 1,
        "occurredAt": scheduled, "producedAt": scheduled, "correlationId": "c-1",
        "payload": {"edition": edition, "status": status, "scheduledFor": scheduled, "missingSources": []},
    }).encode()


class FakeStore:
    def __init__(self) -> None:
        self.runs: list[RunRecord] = []
        self.rows: list[ResultRow] = []

    def exists(self, event_id: str) -> bool:
        return any(r.event_id == event_id for r in self.runs)

    def record(self, run: RunRecord, rows: list[ResultRow]) -> bool:
        if any(r.event_id == run.event_id for r in self.runs):
            return False
        self.runs.append(run)
        self.rows += rows
        return True


class FakeQuery:
    """SQL 본문에 든 이름으로 답을 고른다. 값이 MetricError 이면 던진다."""

    def __init__(self, answers: dict[str, object]) -> None:
        self.answers = answers
        self.calls: list[str] = []

    def scalar(self, sql: str):
        self.calls.append(sql)
        for name, answer in self.answers.items():
            if name in sql:
                if isinstance(answer, MetricError):
                    raise answer
                return answer
        raise AssertionError(f"답이 없는 SQL: {sql}")


class FakeResults:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.puts: list[tuple[str, bytes]] = []

    def put(self, key: str, body: bytes) -> None:
        if self.fail:
            raise UploadError("upload-failed")
        self.puts.append((key, body))


def criteria_file(directory: Path, issue: int, attempt: str, *, metrics: str, days: int = 2, comment: str = "true",
                  extra: str = "") -> Path:
    path = directory / f"{issue}-{attempt}.yml"
    path.write_text(f"issue: {issue}\nattempt: {attempt}\ncomment: {comment}\nfix_commit: \"abc1234\"\n"
                    f"days: {days}\nmetrics:\n{metrics}\n{extra}\n", encoding="utf-8")
    return path


def metric_block(key: str, sql: str, expr: str = "< 0.5") -> str:
    return f"  - key: {key}\n    sql: \"{sql}\"\n    pass: \"{expr}\""
