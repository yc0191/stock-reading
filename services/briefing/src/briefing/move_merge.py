"""장 시작(09:00)부터 아침 판 시작 시각 사이의 급변동을 아침 판에 합치는 단계. Kafka를 모르며 메시지와 시각을 인자로 받는다.

그 시간대의 급변동은 급변동 리포트·메일을 따로 만들지 않고 모아 둔다. 아침 판이 그 시간대 시세를 개장 흐름으로 담기 때문이다.
아침 판 결과가 나오면 다음 중 하나로 끝낸다.
- 아침 판이 나갔고 그 변동의 ETF 개장 흐름이 판에 실제로 담겼으며 변동 시각이 판이 읽은 개장 흐름 구간 안이다:
  작업 기록을 '아침 판에 합침'으로 끝낸다. 리포트는 만들지 않는다.
- 그 밖(판이 실패했거나, 그 ETF의 개장 흐름이 판에 없거나, 변동이 읽은 구간 밖): 급변동 리포트 흐름으로 넘겨
  알림이 사라지지 않게 한다.
아침 판이 진행 중이면 합침 구간이 끝난 뒤 merge_grace가 지나도 기다린다. 판이 시작되지 않았거나 끝났는데 결과가
없으면 같은 식으로 넘긴다.
모아 둔 변동은 메모리에만 있다. 정상 종료 때는 abandon_pending이 '재시작으로 중단' 실패로 남기고, 강제 종료는 작업 기록이
진행 중으로 남아 다음 시작 때 같은 표시가 된다.
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.events import format_timestamp
from briefing.job_log import (
    REASON_INTERRUPTED,
    REASON_MERGED_INTO_MORNING,
    JobKind,
    JobLogPort,
    JobResult,
    JobStart,
    RunStatus,
    move_target,
    record_result,
)
from briefing.jobs import parse_move
from briefing.trace import correlation_context, from_event

log = logging.getLogger("briefing.move_merge")

# 판 결과를 기억하는 날 수. 이보다 오래된 날의 결과는 지운다(메모리가 무한히 늘지 않게).
REMEMBERED_DAYS = 3


@dataclass(frozen=True)
class _Deferred:
    job_id: str
    raw: str | bytes
    symbol: str
    as_of: datetime


@dataclass
class MorningMerge:
    # 아침 판에 합치지 않기로 한 변동을 급변동 리포트 흐름으로 넘긴다(MoveGrouper.on_move)
    release: Callable[[str | bytes, datetime], None]
    job_log: JobLogPort
    settings: DailySettings = DEFAULT_DAILY_SETTINGS
    # 그 날의 아침 판이 진행 중인가. 진행 중이면 기다린다(늦게 성공한 판과 급변동 리포트가 둘 다 나가지 않게).
    morning_in_progress: Callable[[date], bool] = lambda day: False
    _pending: dict[date, list[_Deferred]] = field(default_factory=dict, init=False)
    # 아침 판 결과가 이미 나온 날. 그 뒤 오는 합침 구간의 변동은 합칠 곳이 없으므로 바로 리포트로 간다.
    _resolved: set[date] = field(default_factory=set, init=False)
    # 합침 경로가 본 eventId. 합침으로 끝난 변동이 다시 오면 일반 리포트로 나가지 않게 무시한다.
    _seen: OrderedDict[str, None] = field(default_factory=OrderedDict, init=False)

    def window_end(self, day: date) -> datetime:
        return datetime.combine(day, self.settings.morning_merge_end, tzinfo=KST)

    def in_window(self, at: datetime) -> bool:
        """변동 시각이 장 시작 이상 아침 판 시작 시각 미만인 평일인가."""
        local = at.astimezone(KST)
        return local.weekday() < 5 and self.settings.market_open <= local.time() < self.settings.morning_merge_end

    def on_move(self, message: str | bytes, now: datetime) -> bool:
        """market.move 수신. 합치려고 모아 뒀으면 True, 그렇지 않으면(리포트로 가야 하면) False. 형식이 잘못되면 ValueError."""
        move = parse_move(message, now)
        day = move.as_of.astimezone(KST).date()
        if move.job_id in self._seen:
            log.info("duplicate market.move %s ignored", move.job_id)
            return True
        if not self.in_window(move.as_of) or day in self._resolved \
                or now >= self.window_end(day) + self.settings.morning_merge_grace:
            return False
        self._seen[move.job_id] = None
        while len(self._seen) > self.settings.remembered_move_ids:
            self._seen.popitem(last=False)
        deferred = self._pending.setdefault(day, [])
        raw = message.decode("utf-8") if isinstance(message, bytes) else message
        # 작업은 진행 중으로 시작하고, 아침 판 결과가 나올 때 끝낸다(합침 또는 리포트).
        self.job_log.start(JobStart(move.job_id, JobKind.MOVE, raw,
                                    move_target(move.symbol, format_timestamp(move.as_of)), now))
        deferred.append(_Deferred(move.job_id, raw, move.symbol, move.as_of))
        log.info("market.move held for the morning edition (job=%s)", move.job_id)
        return True

    def on_morning_result(self, scheduled_for: datetime, ok: bool, now: datetime,
                          flowed: frozenset[str] = frozenset(), since: datetime | None = None) -> None:
        """아침 판 결과. ok이고 개장 흐름이 담긴 종목(flowed)의 변동이 읽은 구간(since 이후)에 있으면 합침으로 끝내고,
        나머지는 리포트 흐름으로 넘긴다."""
        day = scheduled_for.astimezone(KST).date()
        self._resolved.add(day)
        for old in [d for d in self._resolved if d < day - timedelta(days=REMEMBERED_DAYS)]:
            self._resolved.discard(old)
        deferred = self._pending.pop(day, [])
        merged = [d for d in deferred if ok and d.symbol in flowed and since is not None and d.as_of >= since]
        for d in merged:
            record_result(self.job_log, JobResult(job_id=d.job_id, status=RunStatus.SUCCEEDED, finished_at=now,
                                                  failure_reason=REASON_MERGED_INTO_MORNING))
        if merged:
            log.info("%d market.move merged into the morning edition", len(merged))
        rest = [d for d in deferred if d not in merged]
        self._release(rest, now, "the morning edition failed" if not ok else "not in the morning edition")

    def on_tick(self, now: datetime) -> None:
        """합침 구간이 끝나고 merge_grace가 지나도 아침 판 결과가 없는 날의 변동을 리포트 흐름으로 넘긴다."""
        for day in [d for d in self._pending if now >= self._due_at(d) and not self.morning_in_progress(d)]:
            self._release(self._pending.pop(day), now, "no morning edition result in time")

    def next_deadline(self) -> datetime | None:
        # 진행 중인 판은 끝나면 on_morning_result가 처리하므로 기한을 세우지 않는다(세우면 기한이 지난 채 계속 깨어난다).
        due = [self._due_at(day) for day in self._pending if not self.morning_in_progress(day)]
        return min(due) if due else None

    def abandon_pending(self, now: datetime) -> list[str]:
        """종료할 때 모아 둔 변동을 비우고 작업을 '재시작으로 중단' 실패로 끝낸다. 리포트는 만들지 않는다. 비운 작업 id를 돌려준다."""
        abandoned = []
        for day in list(self._pending):
            for d in self._pending.pop(day):
                record_result(self.job_log, JobResult(job_id=d.job_id, status=RunStatus.FAILED, finished_at=now,
                                                      failure_reason=REASON_INTERRUPTED))
                abandoned.append(d.job_id)
        return abandoned

    def _due_at(self, day: date) -> datetime:
        return self.window_end(day) + self.settings.morning_merge_grace

    def _release(self, deferred: list[_Deferred], now: datetime, why: str) -> None:
        # 종목코드와 티커는 로그에 쓰지 않는다. 건수와 이유만 남긴다.
        if deferred:
            log.warning("%d held market.move released to the move report flow: %s", len(deferred), why)
        for d in deferred:
            try:
                # 기한이 지나 메시지 처리 밖에서 작업을 시작하므로, 넘기는 변동의 추적 ID를 로그 줄에 직접 건다.
                with correlation_context(from_event(d.raw)):
                    self.release(d.raw, now)
            except ValueError as error:
                # 모아 둘 때 같은 해석을 통과했으므로 일어나지 않는 경로다. 일어나도 다른 변동은 계속 넘긴다.
                log.error("could not release a held market.move: %s", error)
