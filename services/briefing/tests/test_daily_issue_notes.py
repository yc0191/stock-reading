"""ETF 이유의 금리 참고 표시가 내용 없는 금리 이슈 칸을 가리키지 않는지 확인한다."""
import pytest

from briefing.daily_issue_notes import settle_rate_notes
from briefing.daily_report import POINT_ETF, POINT_FX, POINT_OUTLOOK, POINT_RATE, POINT_REVIEW, DailyOutput, Point

NO_RATE_ARTICLE = Point("받은 자료에 금리 관련 기사가 없습니다.", (), "금리 기사 없음", POINT_RATE)
RATE_WITH_ARTICLE = Point("국채금리가 올랐다는 기사가 있습니다.", ("A1",), None, POINT_RATE)
FX = Point("원/달러 환율이 올랐습니다.", ("B1",), None, POINT_FX)


def output(rate, reason):
    return DailyOutput(
        points=(Point(reason, ("A2",), None, POINT_ETF, "TEST01"),),
        insufficient_evidence=False,
        issues=(FX, rate) if rate is not None else (FX,),
        outlooks=(Point(reason, ("A2",), None, POINT_OUTLOOK, "TEST02", "상승"),),
        reviews=(Point(reason, ("A2",), None, POINT_REVIEW, "TEST03"),),
    )


@pytest.mark.parametrize("rate", [NO_RATE_ARTICLE, None], ids=["no-article", "not-written"])
@pytest.mark.parametrize("reason, expected", [
    ("담긴 종목이 영향을 줬을 수 있습니다(금리 이슈 참고).", "담긴 종목이 영향을 줬을 수 있습니다."),
    ("담긴 종목이 영향을 줬을 수 있습니다 (금리 이슈 참고)", "담긴 종목이 영향을 줬을 수 있습니다"),
    ("담긴 종목이 영향을 줬을 수 있습니다(환율·금리 이슈 참고).", "담긴 종목이 영향을 줬을 수 있습니다(환율 이슈 참고)."),
    ("담긴 종목이 영향을 줬을 수 있습니다(환율 이슈 참고).", "담긴 종목이 영향을 줬을 수 있습니다(환율 이슈 참고)."),
])
def test_a_rate_note_is_dropped_when_the_rate_section_has_no_article(rate, reason, expected):
    settled = settle_rate_notes(output(rate, reason))

    assert [p.statement for p in (*settled.points, *settled.outlooks, *settled.reviews)] == [expected] * 3
    assert settled.issues == output(rate, reason).issues


def test_a_rate_note_stays_when_the_rate_section_cites_an_article():
    given = output(RATE_WITH_ARTICLE, "담긴 종목이 영향을 줬을 수 있습니다(환율·금리 이슈 참고).")

    assert settle_rate_notes(given) == given


def test_a_reason_that_is_only_the_rate_note_is_left_as_it_is():
    # 지우면 빈 이유가 되므로 그대로 둔다(LLM이 이렇게 쓸 일은 거의 없다).
    settled = settle_rate_notes(output(NO_RATE_ARTICLE, "(금리 이슈 참고)"))

    assert [p.statement for p in settled.points] == ["(금리 이슈 참고)"]
