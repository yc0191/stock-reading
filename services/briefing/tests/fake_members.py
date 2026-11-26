"""MemberPort 가짜. 넘어온 판 종류마다 정해 둔 멤버 목록을 돌려준다. portfolios가 None이면 읽지 못한 것처럼 행동한다.
국내 ETF인지는 etfs로 정하고, 주지 않으면 멤버 목록의 ETF를 국내 ETF로 본다."""
from briefing.daily_members import MemberPortfolio

# 테스트가 쓰는 종목코드 대부분(TEST01~TEST20)을 보유로 가진 멤버 한 명. 멤버를 가르지 않는 기존 흐름 테스트의 기본값이다.
DEFAULT_MEMBER_ID = 1
DEFAULT_SYMBOLS = tuple(f"TEST{n:02d}" for n in range(1, 21))


def member(member_id, held=(), watched=(), stocks=()):
    """stocks는 국내 주식 (종목코드, 이름) 목록이다."""
    return MemberPortfolio(member_id, tuple([(s, "HELD") for s in held] + [(s, "WATCH") for s in watched]),
                           tuple(stocks))


class FakeMembers:
    def __init__(self, portfolios=..., etfs=...):
        self.portfolios = (member(DEFAULT_MEMBER_ID, DEFAULT_SYMBOLS),) if portfolios is ... else portfolios
        self.etfs = etfs
        self.asked = []

    def daily_members(self, edition):
        self.asked.append(edition)
        return None if self.portfolios is None else tuple(self.portfolios)

    def is_domestic_etf(self, symbol):
        if self.etfs is not ...:
            return None if self.etfs is None else symbol in self.etfs
        return None if self.portfolios is None else any(symbol == s for m in self.portfolios for s, _ in m.etfs)
