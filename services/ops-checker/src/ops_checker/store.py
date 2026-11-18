"""확인기 DB(ops 스키마 쓰기, 지표는 market·briefing 읽기 전용). 표는 infra/db/ops/ 마이그레이션이 만든다."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

import psycopg
from psycopg import errors

from ops_checker.metrics import SQL_TIMEOUT_SECONDS, MetricError

log = logging.getLogger("ops-checker")


@dataclass(frozen=True)
class RunRecord:
    day: date
    edition: str
    event_id: str
    market_status: str
    deploy_version: str
    started_at: datetime
    finished_at: datetime
    outcome: str  # ok / holiday / error
    error: str | None = None


@dataclass(frozen=True)
class ResultRow:
    issue: int
    attempt: str
    criteria_hash: str
    metric: str
    value: Decimal | None
    threshold: str
    verdict: str
    reason: str | None = None


class RunStore(Protocol):
    def exists(self, event_id: str) -> bool: ...

    def record(self, run: RunRecord, rows: list[ResultRow]) -> bool:
        """실행 한 줄과 지표 줄들을 한 트랜잭션에 쓴다. 같은 event_id 가 이미 있으면 쓰지 않고 False."""


class PostgresRunStore:
    def __init__(self, connect) -> None:
        self._connect = connect
        self._conn: psycopg.Connection | None = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    def exists(self, event_id: str) -> bool:
        conn = self._connection()
        try:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute("SELECT 1 FROM ops.check_run WHERE event_id = %s", (event_id,))
                return cur.fetchone() is not None
        except psycopg.OperationalError:
            self._conn = None  # 끊긴 연결은 다음 호출에서 새로 연다
            raise

    def record(self, run: RunRecord, rows: list[ResultRow]) -> bool:
        conn = self._connection()
        try:
            return self._record(conn, run, rows)
        except psycopg.OperationalError:
            self._conn = None
            raise

    @staticmethod
    def _record(conn: psycopg.Connection, run: RunRecord, rows: list[ResultRow]) -> bool:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ops.check_run (day, edition, event_id, market_status, deploy_version, started_at,"
                " finished_at, outcome, error) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                " ON CONFLICT (event_id) DO NOTHING RETURNING id",
                (run.day, run.edition, run.event_id, run.market_status, run.deploy_version, run.started_at,
                 run.finished_at, run.outcome, run.error))
            inserted = cur.fetchone()
            if inserted is None:
                return False
            for row in rows:
                cur.execute(
                    "INSERT INTO ops.check_result (run_id, issue, attempt, criteria_hash, metric, value, threshold,"
                    " verdict, reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (inserted[0], row.issue, row.attempt, row.criteria_hash, row.metric, row.value, row.threshold,
                     row.verdict, row.reason))
        return True


class PostgresScalarQuery:
    """지표 SQL 을 읽기 전용 트랜잭션에서 SQL 하나당 10초 상한으로 돌린다. 끝나면 언제나 되돌린다."""

    def __init__(self, connect) -> None:
        self._connect = connect
        self._conn: psycopg.Connection | None = None

    def scalar(self, sql: str) -> list[tuple]:
        try:
            if self._conn is None or self._conn.closed:
                self._conn = self._connect()
                self._conn.read_only = True
            with self._conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {SQL_TIMEOUT_SECONDS * 1000}")
                cur.execute(sql)
                return cur.fetchmany(2)
        except errors.QueryCanceled as error:
            raise MetricError("timeout") from error
        except errors.ReadOnlySqlTransaction as error:
            raise MetricError("read-only-violation") from error
        except psycopg.Error as error:
            raise MetricError("sql-failed") from error
        finally:
            if self._conn is not None:
                try:
                    self._conn.rollback()
                except psycopg.Error:
                    self._conn = None  # 끊긴 연결은 다음 지표에서 새로 연다


def connector(host: str, port: int, dbname: str, user: str, password: str):
    def connect() -> psycopg.Connection:
        return psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password,
                               connect_timeout=10)
    return connect
