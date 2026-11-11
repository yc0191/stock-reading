"""MemberPort 가짜. 넘어온 판 종류마다 정해 둔 멤버 목록을 돌려준다. portfolios가 None이면 읽지 못한 것처럼 행동한다."""
from briefing.daily_members import MemberPortfolio

# 테스트가 쓰는 종목코드 대부분(TEST01~TEST20)을 보유로 가진 멤버 한 명. 멤버를 가르지 않는 기존 흐름 테스트의 기본값이다.
DEFAULT_MEMBER_ID = 1
DEFAULT_SYMBOLS = tuple(f"TEST{n:02d}" for n in range(1, 21))


def member(member_id, held=(), watched=()):
    return MemberPortfolio(member_id, tuple([(s, "HELD") for s in held] + [(s, "WATCH") for s in watched]))


class FakeMembers:
    def __init__(self, portfolios=...):
        self.portfolios = (member(DEFAULT_MEMBER_ID, DEFAULT_SYMBOLS),) if portfolios is ... else portfolios
        self.asked = []

    def daily_members(self, edition):
        self.asked.append(edition)
        return None if self.portfolios is None else tuple(self.portfolios)
