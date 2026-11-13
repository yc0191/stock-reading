"""JobLogPort 가짜. 넘어온 줄을 그대로 모은다. fail=True면 모든 저장이 끝내 실패한 것처럼, fail_reads=True면 모든 읽기가 실패한 것처럼 행동한다.

entries는 job_log 줄(일마다 한 줄), starts·finishes는 job_run의 시작 줄과 끝 줄이다.
"""
import json

from briefing.compose import KST
from briefing.job_log import EntryType, parse_move_run


class FakeJobLog:
    def __init__(self, fail=False, fail_reads=False):
        self.fail_reads = fail_reads
        self.entries = []
        self.starts = []
        self.finishes = []
        self.fail = fail
        self._failed = set()
        # fail_unfinished를 부른 시각들과, 그때 돌려줄 이전 실행의 작업 id
        self.unfinished_calls = []
        self.unfinished_ids = []
        # 휴장 기록(market_closed_edition): (한국 날짜, 판 값)
        self.closed = set()

    def start(self, run):
        if self.fail:
            self._failed.add(run.job_id)
            return False
        self.starts.append(run)
        return True

    def record(self, entry):
        if self.fail:
            self._failed.add(entry.job_id)
            return False
        self.entries.append(entry)
        return True

    def finish(self, result):
        if self.fail:
            self._failed.add(result.job_id)
            return False
        self.finishes.append(result)
        return True

    def fail_unfinished(self, now):
        self.unfinished_calls.append(now)
        return list(self.unfinished_ids)

    @property
    def failed(self):
        """끝내 실패한 줄만 모은 목록"""
        return self.of_type(EntryType.STEP_FAILED)

    def has_failed(self, job_id):
        return job_id in self._failed

    def morning_outlooks(self, day):
        """그날(한국 시간) 아침 판 작업이 남긴 etf-outlook 줄의 data, 기록이 늦은 것부터. fail_reads면 읽지 못함(None)."""
        if self.fail_reads:
            return None
        mornings = {s.job_id for s in self.starts if getattr(s.job_kind, "value", s.job_kind) == "morning"}
        found = [e for e in self.entries if e.entry_type == EntryType.ETF_OUTLOOK and e.job_id in mornings
                 and e.occurred_at.astimezone(KST).date() == day]
        return [dict(e.data) for e in sorted(found, key=lambda e: e.occurred_at, reverse=True)]

    def moves_between(self, since, until):
        if self.fail_reads:
            return None
        finishes = {f.job_id: f for f in self.finishes}
        runs = []
        for s in self.starts:
            if getattr(s.job_kind, "value", s.job_kind) != "move" or not since <= s.started_at <= until:
                continue
            f = finishes.get(s.job_id)
            run = parse_move_run(s.job_id, s.input, json.dumps(dict(s.target)),
                                 f.status.value if f else "running",
                                 f.generated_by.value if f and f.generated_by else None,
                                 f.failure_reason if f else None)
            if run is not None:
                runs.append(run)
        return runs

    def edition_started(self, kind, day):
        """그날(한국 시간) 이 종류의 작업이 시작됐는가. fail_reads면 읽지 못함(None)."""
        if self.fail_reads:
            return None
        kind = getattr(kind, "value", kind)
        return any(getattr(s.job_kind, "value", s.job_kind) == kind and s.started_at.astimezone(KST).date() == day
                   for s in self.starts)

    def mark_market_closed(self, kind, day, event_id, at):
        if self.fail:
            return False
        self.closed.add((day, getattr(kind, "value", kind)))
        return True

    def market_closed(self, kind, day):
        """그날(한국 시간) 이 판의 휴장 기록이 있는가. fail_reads면 읽지 못함(None)."""
        if self.fail_reads:
            return None
        return (day, getattr(kind, "value", kind)) in self.closed

    def of_reason(self, reason):
        return [e for e in self.entries if e.reason == getattr(reason, "value", reason)]

    def of_type(self, entry_type):
        return [e for e in self.entries if e.entry_type == entry_type]

    def results(self):
        """작업 한 줄의 끝 줄(job_run을 고친 값)"""
        return list(self.finishes)

    def failures(self):
        return self.of_type(EntryType.STEP_FAILED)
