"""같은 종목의 market.move를 모아 작업 하나로 넘기는 단계. Kafka와 DB를 모르며 메시지와 시각을 인자로 받는다.

같은 종목에서 변동이 잇따라 나오면 리포트가 여러 통 나가므로, 마지막 변동 뒤 조용한 시간(move_quiet_period) 동안 같은 종목
변동이 더 없으면 모인 변동으로 작업 하나를 만든다. 변동이 계속 와도 첫 변동부터 move_max_wait가 지나면 그때까지 모인 것으로
만들고, 그 뒤 오는 변동은 새 묶음이 된다. 작업은 가장 늦은 변동으로 만든다(작업 id, 변동 시각 T, 가격이 그 변동의 것).
모인 변동 전부는 작업 기록(moves-grouped 줄)에 남긴다. 다른 종목은 따로 묶인다.

묶음은 메모리에만 있다. 정상 종료(SIGTERM 등) 때는 abandon_pending이 대기 묶음을 "재시작으로 중단" 실패 작업으로 남긴다.
강제 종료(kill -9, 전원 꺼짐)만 흔적 없이 사라진다(읽은 위치를 처리 전에 확정하는 정책의 결과. main.py 참고).
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import format_timestamp
from briefing.job_log import (
    REASON_GROUPED,
    REASON_INTERRUPTED,
    STAGE_MOVE_GROUP,
    EntryType,
    JobKind,
    JobLogEntry,
    JobLogPort,
    JobResult,
    JobStart,
    RunStatus,
    move_target,
    record_result,
)
from briefing.jobs import parse_move

log = logging.getLogger("briefing.move_group")


@dataclass
class _Group:
    first_at: datetime
    last_at: datetime
    # 모인 변동. 원문과 묶음 기록에 남길 값(받은 시각 포함)
    raws: list[str | bytes] = field(default_factory=list)
    moves: list[dict] = field(default_factory=list)
    # 작업의 대상(종목과 변동 시각)은 가장 늦은 변동 것이다
    symbol: str = ""
    latest_as_of: datetime | None = None


@dataclass
class MoveGrouper:
    # 모인 변동 중 가장 늦은 것의 원문을 받는다(BriefingCore.on_move)
    sink: Callable[[str | bytes, datetime], None]
    job_log: JobLogPort
    settings: CoreSettings = DEFAULT_SETTINGS
    _groups: dict[str, _Group] = field(default_factory=dict, init=False)
    # 이미 받은 모든 변동의 eventId. 같은 eventId는 한 번만 센다. 메모리가 무한히 늘지 않게 한다.
    _seen: OrderedDict[str, None] = field(default_factory=OrderedDict, init=False)

    def on_move(self, message: str | bytes, now: datetime) -> None:
        """market.move 수신. 형식이 잘못되면 ValueError(묶음에 넣지 않는다)."""
        move = parse_move(message, now)
        # 조용한 시간이나 최대 대기가 이미 지난 묶음은 이 변동이 들어가기 전에 내보낸다. 이 변동은 새 묶음이 된다.
        self.on_tick(now)
        if move.job_id in self._seen:
            log.info("duplicate market.move %s ignored", move.job_id)
            return
        self._seen[move.job_id] = None
        while len(self._seen) > self.settings.remembered_jobs:
            self._seen.popitem(last=False)
        group = self._groups.setdefault(move.symbol, _Group(first_at=now, last_at=now))
        group.last_at = now
        group.symbol = move.symbol
        group.latest_as_of = move.as_of
        group.raws.append(message)
        group.moves.append({
            "eventId": move.job_id,
            "asOf": format_timestamp(move.as_of),
            "previousPrice": move.previous_price,
            "price": move.price,
            "changePercent": move.change_percent,
            "detectionType": move.detection_type,
            "receivedAt": format_timestamp(now),
        })
        # 조용한 시간이 0이면 바로 내보낸다.
        self.on_tick(now)

    def on_tick(self, now: datetime) -> None:
        """시간이 된 묶음을 내보낸다. 주기적으로, 또는 next_deadline 시각에 부른다."""
        for symbol in [s for s, g in self._groups.items() if now >= self._due_at(g)]:
            self._flush(self._groups.pop(symbol), now)

    def next_deadline(self) -> datetime | None:
        due = [self._due_at(g) for g in self._groups.values()]
        return min(due) if due else None

    def _due_at(self, group: _Group) -> datetime:
        return min(group.last_at + self.settings.move_quiet_period, group.first_at + self.settings.move_max_wait)

    def _flush(self, group: _Group, now: datetime) -> None:
        latest = group.moves[-1]["eventId"]
        try:
            self.sink(group.raws[-1], now)
        except ValueError as error:
            # 받을 때 같은 해석을 이미 통과했으므로 일어나지 않는 경로다. 일어나도 수신 루프는 죽이지 않는다.
            log.error("could not start the job for grouped moves: %s", error)
            return
        # 작업(job_run)은 sink가 만든다. 모인 변동 전부를 그 작업에 남긴다.
        self._record_grouped(group, latest, now)
        log.info("started a job from %d grouped market.move event(s) (job=%s)", len(group.moves), latest)

    def abandon_pending(self, now: datetime) -> list[str]:
        """대기 중인 묶음을 모두 꺼내 작업 없이 \"재시작으로 중단\" 실패로만 남긴다. 정상 종료 때 부른다.

        리포트·LLM 호출·이벤트 발행은 하지 않고 기록만 한다. 돌려주는 값은 비운 묶음의 작업 id(가장 늦은 변동의 eventId)다.
        """
        abandoned = []
        for symbol in list(self._groups):
            group = self._groups.pop(symbol)
            latest = group.moves[-1]["eventId"]
            raw = group.raws[-1]
            self.job_log.start(JobStart(
                latest, JobKind.MOVE, raw.decode("utf-8") if isinstance(raw, bytes) else raw,
                move_target(group.symbol, format_timestamp(group.latest_as_of)), now))
            self._record_grouped(group, latest, now)
            record_result(self.job_log, JobResult(job_id=latest, status=RunStatus.FAILED, finished_at=now,
                                                  failure_reason=REASON_INTERRUPTED))
            abandoned.append(latest)
        return abandoned

    def _record_grouped(self, group: _Group, job_id: str, now: datetime) -> None:
        self.job_log.record(JobLogEntry(
            job_id=job_id, entry_type=EntryType.MOVES_GROUPED, stage=STAGE_MOVE_GROUP, reason=REASON_GROUPED,
            occurred_at=now, attempts=len(group.moves),
            data={"moves": group.moves, "firstReceivedAt": format_timestamp(group.first_at),
                  "quietSeconds": int(self.settings.move_quiet_period.total_seconds()),
                  "maxWaitSeconds": int(self.settings.move_max_wait.total_seconds())}))
