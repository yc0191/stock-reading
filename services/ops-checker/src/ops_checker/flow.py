"""판 시작 알림(daily.collection.completed) 한 건의 처리. Kafka는 모르고 메시지·시각·저장소를 인자로 받는다."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from ops_checker.criteria import Criteria, load_all
from ops_checker.metrics import ERROR, MetricResult, ScalarQuery, evaluate
from ops_checker.results import EDITION, CheckEntry, ResultStore, UploadError, build_result, result_key
from ops_checker.store import ResultRow, RunRecord, RunStore

log = logging.getLogger("ops-checker")

KST = timezone(timedelta(hours=9))
STATUS_COLLECTED = "collected"
STATUS_CLOSED = "market-closed"

# 처리 결과. 로그와 시험에서 쓴다.
IGNORED, DUPLICATE, HOLIDAY, OK, FAILED, STALE = "ignored", "duplicate", "holiday", "ok", "error", "stale"

# 기준 파일의 형식이 틀렸을 때 그 파일을 대신하는 지표 이름
FORMAT_METRIC = "_format"


class MalformedNotification(ValueError):
    """알림 해석(_parse)에서만 던진다. 실행부는 이것만 형식이 틀린 알림으로 보고 영구히 건너뛴다."""


class Checker:
    def __init__(self, store: RunStore, query: ScalarQuery, results: ResultStore, criteria_dir: Path,
                 deploy_version: str, clock: Callable[[], datetime] = lambda: datetime.now(KST)) -> None:
        self._store = store
        self._query = query
        self._results = results
        self._criteria_dir = criteria_dir
        self._deploy_version = deploy_version
        self._clock = clock

    def handle(self, message: str | bytes) -> str:
        """알림 하나를 처리한다. 형식이 잘못된 알림은 MalformedNotification. 처리 결과 이름을 돌려준다."""
        event_id, edition, status, day = _parse(message)
        if edition != EDITION:
            return IGNORED  # 아침 판은 이 흐름에서 받기만 한다
        if day != self._clock().astimezone(KST).date():
            # 지표 SQL 은 지금 값을 계산하므로 지난 날짜 알림(처음 기동 때 되감김, 자정 뒤 재시작)은 날짜가 어긋난 결과를 남긴다.
            log.warning("skip notification of another day %s event=%s", day, event_id)
            return STALE
        if self._store.exists(event_id):
            return DUPLICATE
        started = self._clock()
        entries: list[CheckEntry] = []
        if status == STATUS_COLLECTED:
            entries = [self._check(c) for c in load_all(self._criteria_dir)]
        error = None
        try:
            self._results.put(result_key(day), build_result(day, status, self._deploy_version, entries))
        except UploadError as failure:
            error = str(failure)
        outcome = FAILED if error else (HOLIDAY if status == STATUS_CLOSED else OK)
        run = RunRecord(day, edition, event_id, status, self._deploy_version, started, self._clock(), outcome, error)
        if not self._store.record(run, [row for e in entries for row in _rows(e)]):
            return DUPLICATE
        log.info("check run %s day=%s outcome=%s", event_id, day, outcome)
        return outcome

    def _check(self, criteria: Criteria) -> CheckEntry:
        if criteria.error:
            return CheckEntry(criteria, (MetricResult(FORMAT_METRIC, "", ERROR, reason=criteria.error),))
        return CheckEntry(criteria, tuple(evaluate(m, self._query) for m in criteria.metrics))


def _rows(entry: CheckEntry) -> list[ResultRow]:
    c = entry.criteria
    return [ResultRow(c.issue, c.attempt, c.criteria_hash, m.key, m.value, m.threshold, m.verdict, m.reason)
            for m in entry.metrics]


def _parse(message: str | bytes) -> tuple[str, str, str, date]:
    try:
        event = json.loads(message)
        event_id = event["eventId"]
        payload = event["payload"]
        edition, status = payload["edition"], payload["status"]
        scheduled = datetime.fromisoformat(payload["scheduledFor"])
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
        raise MalformedNotification(f"malformed daily.collection.completed: {type(error).__name__}") from error
    if not isinstance(event_id, str) or not event_id or status not in (STATUS_COLLECTED, STATUS_CLOSED):
        raise MalformedNotification("malformed daily.collection.completed")
    if scheduled.tzinfo is None:
        raise MalformedNotification("scheduledFor without offset")
    return event_id, edition, status, scheduled.astimezone(KST).date()
