"""작업 기록을 briefing 스키마의 테이블 둘에 저장한다(JobLogPort의 DB 구현).

job_run은 작업마다 한 줄(시작 때 만들고 끝날 때 고침), job_log는 그 작업에서 일어난 일마다 한 줄이다.
DB 연결은 connect 함수로 주입받는다. 드라이버는 psycopg_connector 안에서만 import하므로 실제 DB 없이 시험할 수 있다.
테이블은 이 코드가 만들지 않는다. Flyway가 services/briefing/db/migration의 번호 파일로 먼저 만들어 둔다.
"""
from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Protocol

from briefing.config import DEFAULT_STORE_SETTINGS, StoreSettings
from briefing.job_log import (
    REASON_INTERRUPTED,
    STAGE_RESTART,
    EntryType,
    JobLogEntry,
    JobResult,
    JobStart,
    RunStatus,
)

log = logging.getLogger("briefing.job_log_store")

RUN_TABLE = "briefing.job_run"
LOG_TABLE = "briefing.job_log"

# 이전 실행이 끝내지 못하고 남긴 작업. started_at 조건은 이번 실행이 시작한 뒤에 만든 작업을 건드리지 않기 위한 것이다.
FAIL_UNFINISHED_SQL = f"""
UPDATE {RUN_TABLE}
SET status = %s, failure_reason = %s, finished_at = %s
WHERE status = %s AND started_at < %s
RETURNING job_id
"""

START_RUN_SQL = f"""
INSERT INTO {RUN_TABLE} (job_id, job_kind, input, status, target, started_at)
VALUES (%s, %s, %s::json, %s, %s::json, %s)
ON CONFLICT (job_id) DO NOTHING
"""

FINISH_RUN_SQL = f"""
UPDATE {RUN_TABLE}
SET status = %s, failure_reason = %s, generated_by = %s, finished_at = %s, final_text = %s,
    weak_count = %s, research_count = %s, result_event_id = %s
WHERE job_id = %s
"""

INSERT_SQL = f"""
INSERT INTO {LOG_TABLE} (job_id, entry_type, stage, sentence_no, round, before, after, reason, error_type, attempts,
                         data, occurred_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::json, %s)
"""

# 저장에 실패한 작업 id를 기억하는 수. 메모리가 무한히 늘지 않게 한다.
REMEMBERED_FAILED_JOBS = 10000


class Connection(Protocol):
    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


def psycopg_connector(conninfo: str, connect_timeout_seconds: int = 3) -> Callable[[], Connection]:
    """실제 Postgres 연결 함수. 연결이 늦어져 수신 루프가 오래 멈추지 않도록 접속 제한 시간을 둔다."""
    import psycopg

    def connect() -> Connection:
        return psycopg.connect(conninfo, connect_timeout=connect_timeout_seconds)

    return connect


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


@dataclass
class PostgresJobLog:
    connect: Callable[[], Connection]
    settings: StoreSettings = DEFAULT_STORE_SETTINGS
    sleep: Callable[[float], None] = field(default=time.sleep)
    _failed: OrderedDict[str, None] = field(default_factory=OrderedDict, init=False)

    def start(self, run: JobStart) -> bool:
        params = (run.job_id, run.job_kind.value, run.input, RunStatus.RUNNING.value, _dumps(run.target),
                  run.started_at)
        return self._save(START_RUN_SQL, params, run.job_id, "start")

    def record(self, entry: JobLogEntry) -> bool:
        params = (entry.job_id, entry.entry_type.value, entry.stage, entry.sentence_no, entry.round, entry.before,
                  entry.after, entry.reason, entry.error_type, entry.attempts, _dumps(entry.data), entry.occurred_at)
        return self._save(INSERT_SQL, params, entry.job_id, f"{entry.stage}/{entry.entry_type.value}")

    def finish(self, result: JobResult) -> bool:
        params = (result.status.value, result.failure_reason,
                  result.generated_by.value if result.generated_by else None, result.finished_at,
                  result.final_text, result.weak_count, result.research_count, result.result_event_id,
                  result.job_id)
        return self._save(FINISH_RUN_SQL, params, result.job_id, f"finish/{result.status.value}")

    def fail_unfinished(self, now: datetime) -> list[str]:
        """진행 중으로 남은 이전 실행의 작업을 모두 실패(재시작으로 중단)로 끝낸다. 리포트는 다시 만들지 않는다.

        저장소에 닿지 못하면 예외를 그대로 낸다. 시작할 때 부르므로 시작이 실패해야 한다.
        """
        params = (RunStatus.FAILED.value, REASON_INTERRUPTED, now, RunStatus.RUNNING.value, now)
        interrupted = [row[0] for row in self._execute_returning(FAIL_UNFINISHED_SQL, params)]
        for job_id in sorted(interrupted):
            self.record(JobLogEntry(
                job_id=job_id, entry_type=EntryType.STEP_FAILED, stage=STAGE_RESTART, reason=REASON_INTERRUPTED,
                occurred_at=now, error_type="InterruptedByRestart", attempts=1,
                data={"detail": "the previous run ended before this job finished; the report was not remade"}))
        if interrupted:
            log.warning("marked %d unfinished job(s) from the previous run as failed", len(interrupted))
        return interrupted

    def has_failed(self, job_id: str) -> bool:
        return job_id in self._failed

    def _save(self, sql: str, params: tuple, job_id: str, what: str) -> bool:
        attempts = self.settings.record_attempts
        for attempt in range(1, attempts + 1):
            try:
                self._execute(sql, params)
                return True
            except Exception as error:  # noqa: BLE001 - 드라이버마다 예외 종류가 달라 모두 같은 방식으로 다시 시도한다
                # 예외 메시지에는 접속 정보가 섞일 수 있어 종류 이름만 남긴다. 종목코드와 티커도 쓰지 않는다.
                log.warning("saving job log for job %s failed (attempt %d/%d): %s",
                            job_id, attempt, attempts, type(error).__name__)
                if attempt < attempts:
                    self.sleep(self.settings.record_retry_interval.total_seconds())
        log.error("could not save job log: job=%s what=%s", job_id, what)
        self._failed[job_id] = None
        while len(self._failed) > REMEMBERED_FAILED_JOBS:
            self._failed.popitem(last=False)
        return False

    def _execute_returning(self, sql: str, params: tuple) -> list[tuple]:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = list(cursor.fetchall())
            connection.commit()
            return rows
        finally:
            connection.close()

    def _execute(self, sql: str, params: tuple | None) -> None:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
            connection.commit()
        finally:
            connection.close()
