"""작업 기록을 briefing 스키마의 로그 테이블에 한 줄씩 저장한다(JobLogPort의 DB 구현).

DB 연결은 connect 함수로 주입받는다. 드라이버는 psycopg_connector 안에서만 import하므로 실제 DB 없이 시험할 수 있다.
테이블은 시작할 때 create_table로 만든다. 마이그레이션 도구가 없으므로 테이블 모양을 바꾸면 로컬 DB를 지우고 다시 만든다.
"""
from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from briefing.config import DEFAULT_STORE_SETTINGS, StoreSettings
from briefing.job_log import JobLogEntry

log = logging.getLogger("briefing.job_log_store")

TABLE = "briefing.job_log"

# 대상과 관련 데이터는 받은 응답을 가공하지 않고 담아야 하므로 키 순서와 표기를 바꾸는 JSONB가 아니라 JSON으로 둔다.
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id          BIGSERIAL PRIMARY KEY,
    job_kind    VARCHAR(16) NOT NULL,
    researched  BOOLEAN NOT NULL,
    job_id      VARCHAR(64) NOT NULL,
    target      JSON NOT NULL,
    stage       VARCHAR(48) NOT NULL,
    entry_type  VARCHAR(24) NOT NULL,
    reason      VARCHAR(48) NOT NULL,
    error_type  VARCHAR(64),
    attempts    INTEGER NOT NULL CHECK (attempts >= 0),
    data        JSON NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

CREATE_INDEX_SQL = f"CREATE INDEX IF NOT EXISTS job_log_job_id_idx ON {TABLE} (job_id, id)"

INSERT_SQL = f"""
INSERT INTO {TABLE} (job_kind, researched, job_id, target, stage, entry_type, reason, error_type, attempts, data,
                     occurred_at)
VALUES (%s, %s, %s, %s::json, %s, %s, %s, %s, %s, %s::json, %s)
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
        self._execute(CREATE_TABLE_SQL, None)
        self._execute(CREATE_INDEX_SQL, None)

    def record(self, entry: JobLogEntry) -> bool:
        params = (entry.job_kind, entry.researched, entry.job_id, _dumps(entry.target), entry.stage, entry.entry_type,
                  entry.reason, entry.error_type, entry.attempts, _dumps(entry.data), entry.occurred_at)
        attempts = self.settings.record_attempts
        for attempt in range(1, attempts + 1):
            try:
                self._execute(INSERT_SQL, params)
                return True
            except Exception as error:  # noqa: BLE001 - 드라이버마다 예외 종류가 달라 모두 같은 방식으로 다시 시도한다
                # 예외 메시지에는 접속 정보가 섞일 수 있어 종류 이름만 남긴다. 종목코드와 티커도 쓰지 않는다.
                log.warning("recording job log for job %s failed (attempt %d/%d): %s",
                            entry.job_id, attempt, attempts, type(error).__name__)
                if attempt < attempts:
                    self.sleep(self.settings.record_retry_interval.total_seconds())
        log.error("could not record job log: job=%s kind=%s stage=%s type=%s",
                  entry.job_id, entry.job_kind, entry.stage, entry.entry_type)
        self._failed[entry.job_id] = None
        while len(self._failed) > REMEMBERED_FAILED_JOBS:
            self._failed.popitem(last=False)
        return False

    def has_failed(self, job_id: str) -> bool:
        return job_id in self._failed

    def _execute(self, sql: str, params: tuple | None) -> None:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
            connection.commit()
        finally:
            connection.close()
