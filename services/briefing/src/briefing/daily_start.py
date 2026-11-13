"""데일리 판을 언제 시작할지 정한다. Kafka를 모르며 메시지와 시각을 인자로 받는다.

판은 수집기의 판 시작 알림(daily.collection.completed)을 받아 시작하고, 한도 시각(평일)까지 알림이 없으면 있는 자료로
스스로 시작한다. 휴장 알림을 받은 날은 그 판과 그 판의 한도 시각을 끄고, 재시작해도 끈 채로 두도록 휴장 기록을 DB에 남긴다.
같은 날 같은 판은 한 번만 만들며, 이 실행이 기억하지 못해도(재시작) 작업 기록(job_run)에 그날 그 판이 있으면 만들지 않는다.
판 작업 자체는 DailyCore가 한다.
"""
from __future__ import annotations

import json
import logging
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Callable

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.events import EDITION_MORNING, EDITION_POST_CLOSE, EDITIONS, format_timestamp, parse_timestamp
from briefing.job_log import JobKind, JobLogPort
from briefing.trace import correlation_context

log = logging.getLogger("briefing.daily_start")

DAILY_COLLECTION_COMPLETED = "daily.collection.completed"
STATUS_COLLECTED = "collected"
STATUS_MARKET_CLOSED = "market-closed"
# 판의 자료 없음 목록(missingItems)에 남기는 줄: 수집기가 알린 못 받은 자료, 알림 없이 한도 시각에 시작한 판.
MISSING_SOURCES_ITEM = "수집 못 받은 자료"
STALE_DATA_ITEM = "수집 완료 알림을 받지 못해 자료가 낡았을 수 있음"
# 수집기 missingSources 의 자료 종류 이름을 판에 쓰는 이름. 모르는 이름은 그대로 쓴다.
SOURCE_LABELS = {
    "world-index": "세계지수",
    "overseas-daily-price": "해외 일봉",
    "sec-filings": "미국 공시",
    "daily-price": "일봉",
    "exchange-rate-summary": "환율종합",
    "market-summary": "시장종합",
}


# 판 작업을 시작하는 곳(DailyCore.start): 작업 id, 판, 추적 ID, 판 기준 시각, 시작시킨 입력 원문, 지금, 판에 붙일 빠진 자료.
StartEdition = Callable[[str, str, str, datetime, str, datetime, tuple[str, ...]], None]


@dataclass
class DailyStart:
    _start: StartEdition
    job_log: JobLogPort
    settings: DailySettings = DEFAULT_DAILY_SETTINGS
    _seen: OrderedDict[str, None] = field(default_factory=OrderedDict, init=False)
    # (한국 날짜, 판): 그날 이미 만들었거나 만들지 않기로 한 판, 휴장 알림을 받은 판. 지난 날짜는 check_deadlines가 지운다.
    _made: set[tuple[date, str]] = field(default_factory=set, init=False)
    _closed: set[tuple[date, str]] = field(default_factory=set, init=False)

    def on_collection_ready(self, message: str | bytes, now: datetime) -> None:
        """수집기의 판 시작 알림. 같은 eventId는 한 번만 처리한다. 형식이 잘못되면 ValueError."""
        try:
            event = json.loads(message)
            if event["eventType"] != DAILY_COLLECTION_COMPLETED:
                raise ValueError(f"unexpected eventType: {event['eventType']}")
            payload = event["payload"]
            edition, status = payload["edition"], payload["status"]
            if edition not in EDITIONS or status not in (STATUS_COLLECTED, STATUS_MARKET_CLOSED):
                raise ValueError(f"unknown edition or status: {edition!r} {status!r}")
            scheduled_for = parse_timestamp(payload["scheduledFor"])
            missing = tuple(str(m) for m in payload["missingSources"])
            job_id, correlation_id = event["eventId"], event["correlationId"]
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"malformed {DAILY_COLLECTION_COMPLETED} event: {error!r}") from error
        if job_id in self._seen:
            log.info("duplicate daily notice %s (%s) ignored", job_id, edition)
            return
        self._seen[job_id] = None
        while len(self._seen) > self.settings.remembered_notices:
            self._seen.popitem(last=False)
        day = scheduled_for.astimezone(KST).date()
        if day < now.astimezone(KST).date():
            # 브리핑이 꺼져 있던 동안 쌓인 지난 날의 알림이다. 지난 판을 지금 만들지 않는다.
            log.info("daily %s notice for an earlier day %s ignored", edition, day)
            return
        key = (day, edition)
        if status == STATUS_MARKET_CLOSED:
            self._closed.add(key)
            log.info("market closed on %s: the %s edition and its deadline are off", day, edition)
            if not self.job_log.mark_market_closed(JobKind(edition), day, job_id, now):
                # 이 실행 동안은 메모리로 끈다. 재시작하면 한도 시각에 '낡았을 수 있음' 판이 나갈 수 있다.
                log.warning("could not record that the market was closed for the daily %s on %s", edition, day)
            return
        if key in self._closed:
            log.info("daily %s notice on a market-closed day %s ignored", edition, day)
            return
        notes = (f"{MISSING_SOURCES_ITEM}: {', '.join(SOURCE_LABELS.get(m, m) for m in missing)}",) if missing else ()
        raw = message.decode("utf-8") if isinstance(message, bytes) else message
        self._start_once(key, job_id, edition, correlation_id, scheduled_for, raw, now, notes)

    def check_deadlines(self, now: datetime) -> None:
        """한도 시각이 지났는데 오늘(평일) 그 판을 시작하지 않았고 휴장 알림도 없으면, 있는 자료로 판을 시작한다."""
        local = now.astimezone(KST)
        today = local.date()
        self._made = {k for k in self._made if k[0] >= today}
        self._closed = {k for k in self._closed if k[0] >= today}
        if not self.settings.deadline_enabled or local.weekday() >= 5:
            return
        for edition, at in ((EDITION_MORNING, self.settings.morning_deadline),
                            (EDITION_POST_CLOSE, self.settings.post_close_deadline)):
            key = (today, edition)
            deadline = datetime.combine(today, at, tzinfo=KST)
            if key in self._made or key in self._closed or local < deadline:
                continue
            stamp = format_timestamp(deadline)
            correlation_id = f"daily-{edition}-deadline-{stamp}"
            # 같은 날 같은 판의 한도 시각 작업은 늘 같은 id다.
            job_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"daily-deadline:{edition}:{today.isoformat()}"))
            raw = json.dumps({"trigger": "deadline", "edition": edition, "deadline": stamp})
            with correlation_context(correlation_id):
                log.warning("no %s for the %s edition by %s; starting with the data at hand",
                            DAILY_COLLECTION_COMPLETED, edition, stamp)
                self._start_once(key, job_id, edition, correlation_id, deadline, raw, now, (STALE_DATA_ITEM,))

    def _start_once(self, key: tuple[date, str], job_id: str, edition: str, correlation_id: str,
                    scheduled_for: datetime, raw: str, now: datetime, notes: tuple[str, ...]) -> None:
        """같은 날 같은 판은 한 번만 만든다. 이 실행이 기억하지 못해도(재시작) DB에 그날 그 판의 휴장 기록이나 작업 기록이 있으면
        만들지 않는다."""
        day = key[0]
        if key in self._made:
            log.info("daily %s for %s already started; this start is ignored: job=%s", edition, day, job_id)
            return
        closed = self.job_log.market_closed(JobKind(edition), day)
        if closed:
            self._closed.add(key)
            log.info("market closed on %s by the record; the daily %s is not made: job=%s", day, edition, job_id)
            return
        if closed is None:
            log.warning("could not read whether the market was closed for the daily %s on %s; going on: job=%s",
                        edition, day, job_id)
        made = self.job_log.edition_started(JobKind(edition), day)
        self._made.add(key)
        if made:
            log.info("daily %s for %s is already in the job log; this start is ignored: job=%s", edition, day, job_id)
            return
        if made is None:
            # 읽지 못하면 판을 빠뜨리는 것보다 두 번 만드는 편을 택한다.
            log.warning("could not read whether the daily %s for %s was made; making it: job=%s", edition, day, job_id)
        self._start(job_id, edition, correlation_id, scheduled_for, raw, now, notes)
