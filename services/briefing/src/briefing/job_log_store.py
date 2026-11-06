"""작업 기록을 briefing 스키마의 테이블 둘에 저장한다(JobLogPort의 DB 구현).

job_run은 작업마다 한 줄(시작 때 만들고 끝날 때 고침), job_log는 그 작업에서 일어난 일마다 한 줄이다.
DB 연결은 connect 함수로 주입받는다. 드라이버는 psycopg_connector 안에서만 import하므로 실제 DB 없이 시험할 수 있다.
테이블은 시작할 때 create_table로 만든다. 마이그레이션 도구가 없으므로 옛 모양의 job_log가 있으면 이름을 바꿔 남겨 두고
새 모양으로 다시 만든다(지우지 않는다).
"""
from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Protocol

from briefing.config import DEFAULT_STORE_SETTINGS, StoreSettings
from briefing.job_log import EntryType, GeneratedBy, JobKind, JobLogEntry, JobResult, JobStart, RunStatus

log = logging.getLogger("briefing.job_log_store")

RUN_TABLE = "briefing.job_run"
LOG_TABLE = "briefing.job_log"
# 옛 모양(작업 종류·대상이 줄마다 있던 단일 테이블)의 job_log는 이 이름으로 바꿔 남겨 둔다.
LEGACY_LOG_TABLE = "briefing.job_log_legacy"

# 입력·대상·관련 데이터는 받은 내용을 가공하지 않고 담아야 하므로 키 순서와 표기를 바꾸는 JSONB가 아니라 JSON으로 둔다.
# 종류·상태·작성 방식·기록 종류의 값 제한은 글자로 적지 않고 시작할 때 코드 enum으로 건다(CHECKED_COLUMNS).
CREATE_RUN_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {RUN_TABLE} (
    job_id          VARCHAR(64) PRIMARY KEY,
    job_kind        VARCHAR(16) NOT NULL,
    input           JSON NOT NULL,
    status          VARCHAR(16) NOT NULL,
    failure_reason  VARCHAR(48),
    generated_by    VARCHAR(16),
    target          JSON NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    final_text      TEXT,
    weak_count      INTEGER NOT NULL DEFAULT 0 CHECK (weak_count >= 0),
    research_count  INTEGER NOT NULL DEFAULT 0 CHECK (research_count >= 0),
    result_event_id VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

CREATE_LOG_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    id          BIGSERIAL PRIMARY KEY,
    job_id      VARCHAR(64) NOT NULL REFERENCES {RUN_TABLE} (job_id),
    entry_type  VARCHAR(24) NOT NULL,
    stage       VARCHAR(48) NOT NULL,
    sentence_no INTEGER,
    round       INTEGER,
    before      TEXT,
    after       TEXT,
    reason      VARCHAR(48) NOT NULL,
    error_type  VARCHAR(64),
    attempts    INTEGER NOT NULL CHECK (attempts >= 0),
    data        JSON NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

CREATE_INDEXES_SQL = (
    f"CREATE INDEX IF NOT EXISTS job_log_job_id_idx ON {LOG_TABLE} (job_id, id)",
    f"CREATE INDEX IF NOT EXISTS job_run_result_event_id_idx ON {RUN_TABLE} (result_event_id)",
)

# (테이블, 칸, 값의 원본 enum). 시작할 때마다 제약을 지우고 enum으로 다시 건다.
CHECKED_COLUMNS: tuple[tuple[str, str, type[Enum]], ...] = (
    (RUN_TABLE, "job_kind", JobKind),
    (RUN_TABLE, "status", RunStatus),
    (RUN_TABLE, "generated_by", GeneratedBy),
    (LOG_TABLE, "entry_type", EntryType),
)


def check_constraint_sqls(table: str, column: str, values: type[Enum]) -> tuple[str, str]:
    name = f"{table.split('.')[-1]}_{column}_check"
    allowed = ", ".join(f"'{member.value}'" for member in values)
    return (f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}",
            f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({column} IN ({allowed}))")


# 옛 모양의 job_log인지 알아내는 질문: 새 모양에만 있는 칸이 없으면 옛 모양이다.
LEGACY_LOG_PROBE_SQL = """
SELECT
    EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'briefing' AND table_name = 'job_log'),
    EXISTS (SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'briefing' AND table_name = 'job_log' AND column_name = 'sentence_no')
"""
RENAME_LEGACY_SQLS = (
    f"ALTER TABLE {LOG_TABLE} RENAME TO {LEGACY_LOG_TABLE.split('.')[-1]}",
    "ALTER INDEX IF EXISTS briefing.job_log_job_id_idx RENAME TO job_log_legacy_job_id_idx",
)

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

    def create_table(self) -> None:
        self._move_legacy_log_aside()
        self._execute(CREATE_RUN_TABLE_SQL, None)
        self._execute(CREATE_LOG_TABLE_SQL, None)
        for sql in CREATE_INDEXES_SQL:
            self._execute(sql, None)
        for table, column, values in CHECKED_COLUMNS:
            for sql in check_constraint_sqls(table, column, values):
                self._execute(sql, None)

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

    def _move_legacy_log_aside(self) -> None:
        exists, is_current = self._query(LEGACY_LOG_PROBE_SQL)[0]
        if exists and not is_current:
            log.warning("renaming the old job_log table to %s", LEGACY_LOG_TABLE)
            for sql in RENAME_LEGACY_SQLS:
                self._execute(sql, None)

    def _query(self, sql: str) -> list[tuple]:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, None)
                return list(cursor.fetchall())
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
