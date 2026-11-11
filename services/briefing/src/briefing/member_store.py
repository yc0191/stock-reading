"""데일리 판을 받을 멤버와 그 포트폴리오를 personal 스키마에서 읽는다(MemberPort의 DB 구현).

브리핑 계정은 멤버 번호·사용 여부·수신 설정·포트폴리오만 읽을 수 있고 메일 주소 열은 읽지 못한다(주소는 발송기가 찾는다).
읽기만 한다. 값은 판을 내보내기 직전에 읽으므로 재시작 없이 바뀐 설정이 반영된다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from briefing.daily_members import MemberPortfolio
from briefing.events import EDITION_MORNING, EDITION_POST_CLOSE
from briefing.job_log_store import Connection

log = logging.getLogger("briefing.member_store")

# 판 종류마다 켜고 끄는 수신 설정 열. 열 이름은 이 표에서만 고르므로 SQL에 이어 붙여도 값이 섞이지 않는다.
RECEIVE_COLUMNS = {EDITION_MORNING: "receive_morning", EDITION_POST_CLOSE: "receive_post_close"}

# 데일리 자료는 국내 ETF만 다룬다(시세처리기가 포트폴리오의 국내 ETF를 모은다). 같은 기준으로 멤버의 ETF를 읽는다.
SELECT_SQL = """
SELECT m.id, e.symbol, p.relation
FROM personal.member m
JOIN personal.member_mail mail ON mail.member_id = m.id
JOIN personal.member_portfolio p ON p.member_id = m.id
JOIN market.etf e ON e.id = p.etf_id
WHERE m.enabled AND mail.enabled AND mail.{column} AND e.market = 'KR'
ORDER BY m.id, e.symbol
"""


@dataclass
class PostgresMemberStore:
    connect: Callable[[], Connection]

    def daily_members(self, edition: str) -> tuple[MemberPortfolio, ...] | None:
        column = RECEIVE_COLUMNS.get(edition)
        if column is None:
            raise ValueError(f"unknown edition: {edition}")
        try:
            connection = self.connect()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(SELECT_SQL.format(column=column))
                    rows = list(cursor.fetchall())
            finally:
                connection.close()
        except Exception as error:  # noqa: BLE001 - 읽지 못하면 받는 사람을 모르므로 호출한 쪽이 실패로 처리한다
            # 예외 메시지에는 접속 정보가 섞일 수 있어 종류 이름만 남긴다.
            log.error("reading the daily members failed: %s", type(error).__name__)
            return None
        etfs: dict[int, list[tuple[str, str]]] = {}
        for member_id, symbol, relation in rows:
            etfs.setdefault(int(member_id), []).append((symbol, relation))
        return tuple(MemberPortfolio(member_id, tuple(items)) for member_id, items in etfs.items())
