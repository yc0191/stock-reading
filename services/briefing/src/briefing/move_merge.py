"""장 시작(09:00)부터 아침 판 시작 시각 사이의 급변동을 버리는 단계. Kafka를 모르며 메시지와 시각을 인자로 받는다.

그 시간대의 급변동은 종류(ETF, 개별 주식)와 상관없이 급변동 리포트·메일을 만들지 않는다. 장 시작 직후의 큰 움직임은
아침 판이 다루는 시간대라 따로 알리지 않기로 했다. 아침 판이 나갔는지, 늦게 받았는지와 상관없이 받는 즉시 작업 기록을
'장 시작 구간이라 버림'(성공, 리포트 없음)으로 끝내므로 모아 두거나 기다리지 않는다. 아침 판 메일에도 이 변동은 들어가지 않는다.
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.events import format_timestamp
from briefing.job_log import (
    REASON_DROPPED_AT_MARKET_OPEN,
    JobKind,
    JobLogPort,
    JobResult,
    JobStart,
    RunStatus,
    move_target,
    record_result,
)
from briefing.move_job import parse_move

log = logging.getLogger("briefing.move_merge")


@dataclass
class MarketOpenDrop:
    job_log: JobLogPort
    settings: DailySettings = DEFAULT_DAILY_SETTINGS
    # 버린 eventId. 같은 변동이 다시 오면 작업 기록을 또 쓰지 않고, 일반 리포트로도 보내지 않는다.
    _seen: OrderedDict[str, None] = field(default_factory=OrderedDict, init=False)

    def in_window(self, at: datetime) -> bool:
        """변동 시각이 장 시작 이상 아침 판 시작 시각 미만인 평일인가."""
        local = at.astimezone(KST)
        return local.weekday() < 5 and self.settings.market_open <= local.time() < self.settings.morning_merge_end

    def on_move(self, message: str | bytes, now: datetime) -> bool:
        """market.move 수신. 장 시작 구간이라 버렸으면 True, 구간 밖이라 리포트로 가야 하면 False. 형식이 잘못되면 ValueError."""
        move = parse_move(message, now)
        if move.job_id in self._seen:
            log.info("duplicate market.move %s ignored", move.job_id)
            return True
        if not self.in_window(move.as_of):
            return False
        self._seen[move.job_id] = None
        while len(self._seen) > self.settings.remembered_move_ids:
            self._seen.popitem(last=False)
        raw = message.decode("utf-8") if isinstance(message, bytes) else message
        # 왜 메일이 없었는지 작업 기록에서 찾을 수 있게 시작과 끝을 함께 남긴다.
        self.job_log.start(JobStart(move.job_id, JobKind.MOVE, raw,
                                    move_target(move.symbol, format_timestamp(move.as_of)), now))
        record_result(self.job_log, JobResult(job_id=move.job_id, status=RunStatus.SUCCEEDED, finished_at=now,
                                              failure_reason=REASON_DROPPED_AT_MARKET_OPEN))
        # 종목코드는 로그에 쓰지 않는다.
        log.info("market.move dropped: it moved between the market open and the morning edition (job=%s)",
                 move.job_id)
        return True
