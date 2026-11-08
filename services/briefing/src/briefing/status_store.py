"""서버 상태 절에 쓰는 저장소(StatusPort의 DB 구현). 수집 완료 알림을 한 줄씩 저장하고 작업 기록을 읽는다.

DB 연결은 connect 함수로 주입받는다. 테이블은 시작할 때 create_table로 만든다.
받은 원문(raw)은 키 순서와 표기를 바꾸지 않도록 JSON(JSONB 아님)으로 그대로 둔다.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from briefing.config import DEFAULT_STORE_SETTINGS, StoreSettings
from briefing.job_log_store import RUN_TABLE, Connection, _dumps
from briefing.status import CollectionRecord, JobRunRow

log = logging.getLogger("briefing.status_store")

COLLECTION_TABLE = "briefing.collection_completed"

CREATE_COLLECTION_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {COLLECTION_TABLE} (
    event_id           VARCHAR(64) PRIMARY KEY,
    job_type           VARCHAR(48) NOT NULL,
    scheduled_for      TIMESTAMPTZ NOT NULL,
    started_at         TIMESTAMPTZ NOT NULL,
    finished_at        TIMESTAMPTZ NOT NULL,
    published_count    INTEGER NOT NULL CHECK (published_count >= 0),
    incomplete_symbols JSON NOT NULL,
    research_job_id    VARCHAR(64),
    raw                JSON NOT NULL,
    received_at        TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""
CREATE_COLLECTION_INDEX_SQL = (
    f"CREATE INDEX IF NOT EXISTS collection_completed_finished_at_idx ON {COLLECTION_TABLE} (finished_at)"
)

INSERT_COLLECTION_SQL = f"""
INSERT INTO {COLLECTION_TABLE} (event_id, job_type, scheduled_for, started_at, finished_at, published_count,
                                incomplete_symbols, research_job_id, raw)
VALUES (%s, %s, %s, %s, %s, %s, %s::json, %s, %s::json)
ON CONFLICT (event_id) DO NOTHING
"""

SELECT_COLLECTIONS_SQL = f"""
SELECT event_id, job_type, scheduled_for, started_at, finished_at, published_count, incomplete_symbols::text,
       research_job_id, raw::text
FROM {COLLECTION_TABLE}
WHERE finished_at >= %s AND finished_at <= %s
ORDER BY finished_at, event_id
"""

SELECT_RUNS_SQL = f"""
SELECT job_id, job_kind, status, failure_reason, generated_by, started_at
FROM {RUN_TABLE}
WHERE started_at >= %s AND started_at <= %s
ORDER BY started_at, job_id
"""


@dataclass
class PostgresStatusStore:
    connect: Callable[[], Connection]
    settings: StoreSettings = DEFAULT_STORE_SETTINGS
    sleep: Callable[[float], None] = field(default=time.sleep)

    def create_table(self) -> None:
        self._execute(CREATE_COLLECTION_TABLE_SQL, None)
        self._execute(CREATE_COLLECTION_INDEX_SQL, None)

    def save_collection(self, record: CollectionRecord) -> bool:
        params = (record.event_id, record.job_type, record.scheduled_for, record.started_at, record.finished_at,
                  record.published_count, _dumps(list(record.incomplete_symbols)), record.research_job_id,
                  record.raw)
        attempts = self.settings.record_attempts
        for attempt in range(1, attempts + 1):
            try:
                self._execute(INSERT_COLLECTION_SQL, params)
                return True
            except Exception as error:  # noqa: BLE001 - 드라이버마다 예외 종류가 달라 모두 같은 방식으로 다시 시도한다
                # 예외 메시지에는 접속 정보가 섞일 수 있어 종류 이름만 남긴다.
                log.warning("saving collection.completed failed (attempt %d/%d): %s",
                            attempt, attempts, type(error).__name__)
                if attempt < attempts:
                    self.sleep(self.settings.record_retry_interval.total_seconds())
        log.error("could not save collection.completed: event=%s", record.event_id)
        return False

    def collections_between(self, since: datetime, until: datetime) -> list[CollectionRecord] | None:
        rows = self._query(SELECT_COLLECTIONS_SQL, (since, until))
        if rows is None:
            return None
        return [CollectionRecord(r[0], r[1], r[2], r[3], r[4], int(r[5]), tuple(json.loads(r[6])), r[7], r[8])
                for r in rows]

    def job_runs_between(self, since: datetime, until: datetime) -> list[JobRunRow] | None:
        rows = self._query(SELECT_RUNS_SQL, (since, until))
        if rows is None:
            return None
        return [JobRunRow(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]

    def _query(self, sql: str, params: tuple) -> list[tuple] | None:
        try:
            connection = self.connect()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)
                    return list(cursor.fetchall())
            finally:
                connection.close()
        except Exception as error:  # noqa: BLE001 - 조회 실패는 '자료 없음'으로 쓰고 판은 그대로 나간다
            log.warning("reading the server status data failed: %s", type(error).__name__)
            return None

    def _execute(self, sql: str, params: tuple | None) -> None:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
            connection.commit()
        finally:
            connection.close()
