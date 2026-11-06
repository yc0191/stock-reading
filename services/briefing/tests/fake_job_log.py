"""JobLogPort 가짜. 넘어온 줄을 그대로 모은다. fail=True면 모든 저장이 끝내 실패한 것처럼 행동한다.

entries는 job_log 줄(일마다 한 줄), starts·finishes는 job_run의 시작 줄과 끝 줄이다.
"""
from briefing.job_log import EntryType


class FakeJobLog:
    def __init__(self, fail=False):
        self.entries = []
        self.starts = []
        self.finishes = []
        self.fail = fail
        self._failed = set()

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

    @property
    def failed(self):
        """끝내 실패한 줄만 모은 목록"""
        return self.of_type(EntryType.STEP_FAILED)

    def has_failed(self, job_id):
        return job_id in self._failed

    def of_reason(self, reason):
        return [e for e in self.entries if e.reason == getattr(reason, "value", reason)]

    def of_type(self, entry_type):
        return [e for e in self.entries if e.entry_type == entry_type]

    def results(self):
        """작업 한 줄의 끝 줄(job_run을 고친 값)"""
        return list(self.finishes)

    def failures(self):
        return self.of_type(EntryType.STEP_FAILED)
