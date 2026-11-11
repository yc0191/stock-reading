import pytest

from briefing.daily_jobs import DailyCore
from fake_members import FakeMembers


@pytest.fixture(autouse=True)
def default_daily_members(monkeypatch):
    """DailyCore를 members 없이 만드는 테스트에는 멤버 한 명(TEST01~TEST20 보유)이 있는 것으로 한다.
    멤버를 따지는 테스트는 members=를 직접 넘긴다."""
    original = DailyCore.__init__

    def init(self, *args, **kwargs):
        if len(args) < 6 and "members" not in kwargs:
            kwargs["members"] = FakeMembers()
        original(self, *args, **kwargs)

    monkeypatch.setattr(DailyCore, "__init__", init)
