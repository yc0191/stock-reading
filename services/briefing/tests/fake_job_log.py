"""JobLogPort 가짜. 넘어온 줄을 그대로 모은다. fail=True면 모든 저장이 끝내 실패한 것처럼 행동한다."""
from briefing.job_log import TYPE_FAILED, TYPE_RESULT


class FakeJobLog:
    def __init__(self, fail=False):
        self.entries = []
        self.fail = fail
        self._failed = set()

    def record(self, entry):
        if self.fail:
            self._failed.add(entry.job_id)
            return False
        self.entries.append(entry)
        return True

    @property
    def failed(self):
        """끝내 실패한 줄만 모은 목록"""
        return [e for e in self.entries if e.entry_type == TYPE_FAILED]

    def has_failed(self, job_id):
        return job_id in self._failed

    def of_reason(self, reason):
        return [e for e in self.entries if e.reason == reason]

    def of_type(self, entry_type):
        return [e for e in self.entries if e.entry_type == entry_type]

    def results(self):
        return self.of_type(TYPE_RESULT)

    def failures(self):
        return self.of_type(TYPE_FAILED)
