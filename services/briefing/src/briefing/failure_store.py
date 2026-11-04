"""리포트를 만들지 못한 변동을 briefing 스키마의 실패 테이블에 한 줄씩 기록한다(FailurePort의 DB 구현).

DB 연결은 connect 함수로 주입받는다. 드라이버는 psycopg_connector 안에서만 import하므로 실제 DB 없이 시험할 수 있다.
테이블은 시작할 때 create_table로 만든다. 마이그레이션 도구가 없으므로 테이블 모양을 바꾸면 로컬 DB를 지우고 다시 만든다.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from briefing.config import DEFAULT_STORE_SETTINGS, StoreSettings
from briefing.ports import Failure, FailureReason

log = logging.getLogger("briefing.failure_store")

TABLE = "briefing.failed_jobs"
# 실패 이유를 알아볼 정도의 짧은 설명만 남긴다. 기사 내용 같은 긴 값이 들어오지 않게 길이를 묶는다.
DETAIL_MAX_LENGTH = 500

_REASONS = ", ".join(f"'{reason.value}'" for reason in FailureReason)
# 이름 없는 열 CHECK에 Postgres가 붙이는 이름과 같다. 이전에 만든 테이블도 이 이름으로 찾아 바꾼다.
REASON_CHECK = "failed_jobs_reason_check"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    job_id     VARCHAR(64) PRIMARY KEY,
    symbol     VARCHAR(16) NOT NULL,
    reason     VARCHAR(32) NOT NULL CONSTRAINT {REASON_CHECK} CHECK (reason IN ({_REASONS})),
    detail     VARCHAR({DETAIL_MAX_LENGTH}) NOT NULL,
    attempts   INTEGER NOT NULL CHECK (attempts >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# 사유가 늘면 이미 있는 테이블은 CREATE TABLE IF NOT EXISTS로 바뀌지 않으므로 제약을 지금 사유 목록으로 다시 건다.
UPDATE_REASONS_SQL = f"""
ALTER TABLE {TABLE}
    DROP CONSTRAINT IF EXISTS {REASON_CHECK},
    ADD CONSTRAINT {REASON_CHECK} CHECK (reason IN ({_REASONS}))
"""

INSERT_SQL = f"""
INSERT INTO {TABLE} (job_id, symbol, reason, detail, attempts)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (job_id) DO NOTHING
"""


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


def _truncate(detail: str) -> str:
    return detail if len(detail) <= DETAIL_MAX_LENGTH else detail[:DETAIL_MAX_LENGTH]


@dataclass
class PostgresFailureStore:
    connect: Callable[[], Connection]
    settings: StoreSettings = DEFAULT_STORE_SETTINGS
    sleep: Callable[[float], None] = field(default=time.sleep)

    def create_table(self) -> None:
        self._execute(CREATE_TABLE_SQL, None)
        self._execute(UPDATE_REASONS_SQL, None)

    def record(self, failure: Failure) -> None:
        params = (failure.job_id, failure.symbol, failure.reason.value, _truncate(failure.detail), failure.attempts)
        attempts = self.settings.record_attempts
        for attempt in range(1, attempts + 1):
            try:
                if self._execute(INSERT_SQL, params) == 0:
                    log.info("failure for job %s was already recorded", failure.job_id)
                return
            except Exception as error:  # noqa: BLE001 - 드라이버마다 예외 종류가 달라 모두 같은 방식으로 다시 시도한다
                # 예외 메시지에는 접속 정보가 섞일 수 있어 종류 이름만 남긴다.
                log.warning("recording failure for job %s failed (attempt %d/%d): %s",
                            failure.job_id, attempt, attempts, type(error).__name__)
                if attempt < attempts:
                    self.sleep(self.settings.record_retry_interval.total_seconds())
        log.error("could not record failure: job=%s symbol=%s reason=%s",
                  failure.job_id, failure.symbol, failure.reason.value)

    def _execute(self, sql: str, params: tuple | None) -> int:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rowcount = cursor.rowcount
            connection.commit()
            return rowcount
        finally:
            connection.close()
