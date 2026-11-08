"""근거 조회 흐름의 설정값. 코드에 숫자를 박지 않고 여기서만 정한다."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class CoreSettings:
    # 변동 시각 T 이후 이 시간 안에 나온 기사·시세도 근거 조회 구간에 넣는다.
    delta: timedelta = timedelta(minutes=5)
    evidence_window: timedelta = timedelta(hours=24)
    market_lookback: timedelta = timedelta(minutes=5)
    response_timeout: timedelta = timedelta(seconds=10)
    max_attempts: int = 3
    recheck_interval: timedelta = timedelta(seconds=10)
    max_rounds: int = 5
    collection_wait_limit: timedelta = timedelta(minutes=30)
    recent_completions: int = 10
    # 시세 이력은 T−5분~T+Δ 구간을 이 간격으로 솎아 받는다. 구간은 양끝을 포함하므로 10분 구간을 10초로 나누면
    # 61칸이다. 상한이 칸 수보다 작으면 마지막 칸이 늘 잘리고 LLM 입력에 잘림이 붙는다.
    history_interval: timedelta = timedelta(seconds=10)
    max_history_points: int = 61
    max_articles: int = 50
    # 공시는 하루 한 번(장 시작 전) 받으므로 +Δ를 두지 않는다. 미국 금요일 장 마감 뒤 공시를 월요일 변동에서 놓치지 않게 72시간을 본다.
    filings_window: timedelta = timedelta(hours=72)
    max_filings: int = 20
    # 배경 자료(환율·지수·시장종합·일봉)와 해외 등락률은 받은 시각이 이 기간보다 오래되면 없음으로 본다.
    background_window: timedelta = timedelta(days=4)
    max_background_bytes: int = 32 * 1024
    max_tickers: int = 100
    # LLM 입력에 넣는 ETF 구성 종목 수. 비중 순위가 높은 종목부터 이 수까지 넣는다.
    max_etf_members: int = 10
    # 같은 eventId 중복 처리를 위해 기억하는 작업 키 수. 메모리가 무한히 늘지 않게 한다.
    remembered_jobs: int = 10000
    # 근거 부족 시 재검색: 검색어 LLM이 낼 수 있는 검색어 수, 검색어 LLM 시도 횟수, 재검색 완료 알림을 기다리는 상한.
    # 재검색 기사 조회는 위의 응답 제한 시간·시도 횟수·재확인 간격·라운드 상한을 그대로 쓴다.
    max_research_queries: int = 3
    research_query_attempts: int = 3
    research_wait_limit: timedelta = timedelta(minutes=5)
    # 근거가 약한 문장 단위 재검색: 문장 하나를 찾는 최대 횟수(처음 찾기 포함), 작업 하나가 보낼 수 있는 재검색 요청 합계,
    # 작업 하나의 재검색 전체 시간. 요청 합계를 넘는 문장은 찾지 않고 '근거 약함'을 남기고, 전체 시간이 지나면 그때의 본문으로 낸다.
    research_rounds_per_sentence: int = 2
    max_research_requests: int = 10
    research_total_limit: timedelta = timedelta(minutes=15)


@dataclass(frozen=True)
class StoreSettings:
    # 실패 기록 저장은 수신 루프 안에서 불리므로, 다시 시도해도 루프가 오래 멈추지 않게 간격을 짧게 둔다.
    record_attempts: int = 3
    record_retry_interval: timedelta = timedelta(milliseconds=500)


DEFAULT_SETTINGS = CoreSettings()
DEFAULT_STORE_SETTINGS = StoreSettings()


@dataclass(frozen=True)
class DailySettings:
    """데일리 브리핑(아침 판, 마감 후 판)의 설정값."""

    # 범위 시작은 예정 시각에서 직전 평일의 같은 시각이다. 그래도 이보다 오래 거슬러 가지 않는다.
    max_lookback: timedelta = timedelta(days=7)
    # 재요청 규칙은 급변동 근거 조회와 같다.
    response_timeout: timedelta = timedelta(seconds=10)
    max_attempts: int = 3
    max_response_bytes: int = 32 * 1024
    max_etfs: int = 50
    max_tickers: int = 100
    max_filings: int = 20
    # LLM 입력에 넣는 ETF 구성 종목 수(ETF마다). 비중 순위가 높은 종목부터 넣는다.
    max_etf_members: int = 10
    # 세계지수 대륙구분 중 미국 지수가 든 값(market 설정 world-index-continents의 아메리카).
    us_index_target: str = "C"
    llm_max_attempts: int = 3
    # 같은 tick 중복 처리를 막기 위해 기억하는 eventId 수.
    remembered_ticks: int = 1000
    # 서버 상태 절의 '주의': 판 시각에 시작한 지 이만큼 넘게 진행 중인 작업은 멈춘 것으로 본다.
    stuck_after: timedelta = timedelta(minutes=30)
    # 메일 본문 이벤트(briefing.daily.generated)의 그림 크기 상한. 그림 한 장(base64를 푼 크기)과 이벤트 전체(JSON 바이트)가
    # 넘으면 그림만 빼고 글자판+HTML로 보낸다. 그림은 한 판에 4장까지다.
    max_chart_images: int = 4
    max_chart_image_bytes: int = 300 * 1024
    max_event_bytes: int = 900 * 1024


DEFAULT_DAILY_SETTINGS = DailySettings()
