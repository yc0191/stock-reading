"""데일리 판을 받을 멤버 읽기와 브리핑 계정의 권한을 실제 Postgres로 확인한다. 핵심은 브리핑 계정이 멤버 번호·수신 설정·포트폴리오는
읽어도 개인정보인 메일 주소(member_mail.email)와 멤버 이름은 읽지 못하고, personal에 쓰지도 못한다는 것이다.

BRIEFING_IT_DB_*(읽는 계정)와 값을 넣는 WRITER_IT_DB_*(personal 쓰기 계정), MARKET_IT_DB_*(market 주인 계정)가 모두 있을 때만 실행하고,
없으면 건너뛴다. 대상은 scripts/test-all.sh가 market → personal 초기화 → personal 마이그레이션 순서로 준비한 테스트 전용 DB다.
다른 테스트가 남긴 멤버와 섞이지 않도록 이 테스트가 만든 멤버 번호만 골라 확인하고, 끝나면 자기 행을 지운다.
"""
import os
import re

import pytest

from briefing.job_log_store import psycopg_connector
from briefing.member_store import PostgresMemberStore

_VARS = ("BRIEFING_IT_DB_URL", "BRIEFING_IT_DB_USER", "BRIEFING_IT_DB_PASSWORD", "WRITER_IT_DB_USER",
         "WRITER_IT_DB_PASSWORD", "MARKET_IT_DB_USER", "MARKET_IT_DB_PASSWORD")

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="실제 DB 테스트: BRIEFING_IT_DB_*·WRITER_IT_DB_*·MARKET_IT_DB_* 가 없음",
)

# 다른 테스트의 종목코드와 겹치지 않는 값(notifier는 NOTI*, market은 TEST*를 쓴다)
KR_ETFS = ("BMEM01", "BMEM02", "BMEM03")
US_ETF = "BMEMUS"
MAIL = "briefing-it-%s@example.com"


def connector(user_key, password_key):
    from psycopg.conninfo import make_conninfo

    return psycopg_connector(make_conninfo(os.environ["BRIEFING_IT_DB_URL"], user=os.environ[user_key],
                                           password=os.environ[password_key]))


def run(connect, sql, params=None, fetch=False):
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall() if fetch else None
        connection.commit()
        return rows
    finally:
        connection.close()


@pytest.fixture(scope="module")
def briefing():
    return connector("BRIEFING_IT_DB_USER", "BRIEFING_IT_DB_PASSWORD")


@pytest.fixture(scope="module")
def world():
    """서로 다른 포트폴리오와 설정을 가진 멤버들. 값은 쓰기 계정과 market 주인 계정이 넣는다."""
    writer = connector("WRITER_IT_DB_USER", "WRITER_IT_DB_PASSWORD")
    market = connector("MARKET_IT_DB_USER", "MARKET_IT_DB_PASSWORD")
    # personal 값을 넣는 테스트이므로 개발용 DB가 아니라 test-all.sh가 만든 일회용 DB에서만 돈다(Java 실제 DB 테스트와 같은 규칙).
    (database,) = run(market, "SELECT current_database()", fetch=True)[0]
    if not re.match(r"^stock_test_run_\d{14}_\d+$", database):
        raise RuntimeError(f"실제 DB 테스트는 stock_test_run_로 시작하는 일회용 데이터베이스에서만 돌 수 있습니다: {database}")
    run(market, "DELETE FROM market.etf WHERE symbol = ANY(%s)", ([*KR_ETFS, US_ETF],))
    run(market, "INSERT INTO market.etf (market, symbol, name) VALUES ('KR', 'BMEM01', 'BMEM ONE'), "
                "('KR', 'BMEM02', 'BMEM TWO'), ('KR', 'BMEM03', 'BMEM THREE'), ('US', 'BMEMUS', 'BMEM US')")
    members = {}

    def add(key, enabled=True, mail_enabled=True, morning=True, post_close=True, held=(), watched=()):
        (member_id,) = run(writer, "INSERT INTO personal.member (name, enabled) VALUES (%s, %s) RETURNING id",
                           (f"BMEM {key}", enabled), fetch=True)[0]
        run(writer, "INSERT INTO personal.member_mail (member_id, email, receive_morning, receive_post_close, enabled) "
                    "VALUES (%s, %s, %s, %s, %s)", (member_id, MAIL % key, morning, post_close, mail_enabled))
        for relation, symbols in (("HELD", held), ("WATCH", watched)):
            for symbol in symbols:
                run(writer, "INSERT INTO personal.member_portfolio (member_id, etf_id, relation) "
                            "SELECT %s, id, %s FROM market.etf WHERE symbol = %s", (member_id, relation, symbol))
        members[key] = member_id

    add("both", held=("BMEM01",), watched=("BMEM02",))
    add("night-only", morning=False, held=("BMEM02", "BMEM03"))
    add("member-off", enabled=False, held=("BMEM01",))
    add("mail-off", mail_enabled=False, held=("BMEM01",))
    add("us-only", held=(US_ETF,))
    add("no-portfolio")
    yield members
    ids = list(members.values())
    run(writer, "DELETE FROM personal.member WHERE id = ANY(%s)", (ids,))  # 메일·포트폴리오 줄은 CASCADE로 따라 지워진다
    run(market, "DELETE FROM market.etf WHERE symbol = ANY(%s)", ([*KR_ETFS, US_ETF],))


def mine(rows, world):
    ids = set(world.values())
    return {m.member_id: m.etfs for m in rows if m.member_id in ids}


def test_the_morning_board_goes_to_enabled_members_with_a_korean_etf_who_receive_it(briefing, world):
    rows = PostgresMemberStore(briefing).daily_members("morning")
    assert mine(rows, world) == {world["both"]: (("BMEM01", "HELD"), ("BMEM02", "WATCH"))}


def test_the_post_close_board_uses_its_own_receive_setting_and_each_members_own_relation(briefing, world):
    rows = PostgresMemberStore(briefing).daily_members("post-close")
    assert mine(rows, world) == {world["both"]: (("BMEM01", "HELD"), ("BMEM02", "WATCH")),
                                 world["night-only"]: (("BMEM02", "HELD"), ("BMEM03", "HELD"))}
    # 같은 ETF(BMEM02)도 멤버마다 보유/관심이 다르게 읽힌다.
    assert dict(mine(rows, world)[world["both"]])["BMEM02"] != dict(mine(rows, world)[world["night-only"]])["BMEM02"]


def denied(connect, sql):
    import psycopg

    with pytest.raises(psycopg.errors.InsufficientPrivilege, match="permission denied"):
        run(connect, sql, fetch=sql.lstrip().upper().startswith("SELECT"))


def test_the_briefing_account_cannot_read_the_member_mail_address_or_the_member_name(briefing, world):
    denied(briefing, "SELECT email FROM personal.member_mail")
    denied(briefing, "SELECT * FROM personal.member_mail")
    denied(briefing, "SELECT name FROM personal.member")
    denied(briefing, "SELECT receive_move FROM personal.member_mail")
    denied(briefing, "SELECT name FROM market.etf")


def test_the_briefing_account_reads_only_the_columns_it_needs(briefing, world):
    assert run(briefing, "SELECT member_id, enabled, receive_morning, receive_post_close FROM personal.member_mail "
                         "WHERE member_id = %s", (world["both"],), fetch=True) == [(world["both"], True, True, True)]
    assert run(briefing, "SELECT id, enabled FROM personal.member WHERE id = %s", (world["both"],), fetch=True) == \
        [(world["both"], True)]
    assert run(briefing, "SELECT symbol FROM market.etf WHERE symbol = 'BMEM01'", fetch=True) == [("BMEM01",)]


def test_the_briefing_account_cannot_write_personal_data_or_read_the_collect_views(briefing, world):
    denied(briefing, "INSERT INTO personal.member (name) VALUES ('BMEM X')")
    denied(briefing, "UPDATE personal.member_mail SET receive_morning = FALSE")
    denied(briefing, "DELETE FROM personal.member_portfolio")
    denied(briefing, "UPDATE market.etf SET name = 'X'")
    denied(briefing, "SELECT * FROM personal.collect_target")
