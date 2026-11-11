"""근거 조회 흐름의 설정값. 코드에 숫자를 박지 않고 여기서만 정한다."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import time, timedelta


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
    # 아침 판의 개장 시세 범위: 판 날짜의 장 시작 시각(한국 시간)부터 판 시각까지. 판이 늦게 시작해도 판 시각에서 이 길이보다
    # 멀리 거슬러 읽지 않는다. 판 시각 뒤에 받은 시세는 쓰지 않는다(상태형 자료).
    open_flow_start: time = time(9, 0)
    open_flow_max_window: timedelta = timedelta(minutes=30)
    max_tickers: int = 100
    # LLM 입력에 넣는 ETF 구성 종목 수. 비중 순위가 높은 종목부터 이 수까지 넣는다.
    max_etf_members: int = 10
    # 같은 종목의 market.move는 바로 처리하지 않고 모은다. 마지막 변동 뒤 이 시간 동안 같은 종목 변동이 더 없으면 모인 변동으로
    # 작업 하나를 만든다. 변동이 계속 와도 첫 변동부터 최대 이 시간이 지나면 그때까지 모인 것으로 만든다(영영 밀리지 않게).
    # 0이면 변동마다 바로 작업을 만든다(기본). 급변동 메일을 변동 뒤 약 10분 안에 보내려는 것이며, 같은 종목 중복은 시세처리기가 막는다.
    move_quiet_period: timedelta = timedelta(0)
    move_max_wait: timedelta = timedelta(minutes=30)
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
    # 변동부터 메일 이벤트까지 약 10분 안에 끝내려고 4분으로 둔다(T+5분 재검색, 근거 조회, 첫 본문 뒤의 시간).
    research_total_limit: timedelta = timedelta(minutes=4)
    # 정기 뉴스 수집을 기다리지 않고, 변동 시각 T 뒤 이 시간이 되면 ETF 이름과 비중 상위 구성 종목 이름으로 먼저 재검색한다.
    # 그 재검색 완료도 정기 수집 완료와 같이 '수집 완료'로 본다. 검색어 수는 max_research_queries를 쓴다.
    early_research_delay: timedelta = timedelta(minutes=5)


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
    # 마감 후 판: ETF마다 요청하는 기사 건수(최신순). 범위는 판 범위와 같다.
    etf_max_articles: int = 5
    # 마감 후 판의 미국 구성 종목 이슈: 비중 상위 몇 개 안의 종목을 보는지, 직전 미국 장 등락률 절댓값(%)이 이 이상이면
    # 이슈, ETF마다 보여 주는 종목 수 상한. 범위 안 공시가 있는 종목도 이슈다.
    member_rank_limit: int = 10
    member_move_percent: float = 3.0
    member_issues_per_etf: int = 3
    # 마감 후 판의 눈에 띄는 점 후보: 업종이 같은 ETF 둘이 반대로 움직였다고 볼 최소 등락률 절댓값(%),
    # 같은 방향으로 크게 움직였다고 볼 등락률 절댓값(%)과 묶음으로 치는 최소 ETF 수.
    opposite_move_percent: float = 0.5
    cluster_move_percent: float = 2.0
    cluster_min_size: int = 3
    # 재검색에서 빼는 '시장과 같은 방향': ETF 등락과 코스피 종합 등락의 부호가 같고 두 등락률 차이(절댓값, %p)가 이 이내일 때만.
    market_same_direction_gap_percent: float = 1.0
    # 세계지수 대륙구분 중 미국 지수가 든 값(market 설정 world-index-continents의 아메리카).
    us_index_target: str = "C"
    # 마감 후 판 기준 시각(한국 시간)이 이보다 이르면 그날 장이 아직 열리지 않은 것으로 본다(일봉이 모두 등락 0.00일 때만).
    market_open: time = time(9, 0)
    # 장 시작부터 이 시각 전까지의 급변동은 급변동 리포트를 따로 만들지 않고 아침 판에 합친다. 아침 판 시작 시각(수집기의
    # collector.daily-briefing.morning-cron 기본 평일 09:10)과 같아야 한다. 합침 구간이 끝난 뒤 이 시간이 지나도 아침 판
    # 결과가 없으면 모아 둔 변동을 급변동 리포트로 넘긴다.
    morning_merge_end: time = time(9, 10)
    # 합침을 켠다. 수집기의 아침 판 tick을 켜는 값(collector.daily-briefing.enabled = COLLECTOR_DAILY_BRIEFING_ENABLED)과
    # 짝이다. tick이 오지 않는데 켜 두면 합침 구간의 급변동이 grace만큼 늦게 나가므로 꺼져 있으면 바로 리포트를 만든다.
    morning_merge_enabled: bool = False
    # 합침 경로가 기억하는 eventId 수. 급변동 묶음기(remembered_jobs)와 같은 상한이다.
    remembered_move_ids: int = 10000
    morning_merge_grace: timedelta = timedelta(minutes=30)
    # 기사가 그 ETF와 관계있는지 볼 때 추종 대상·ETF 이름에서 앞의 나라 이름과 끝의 접미어를 뗀 것도 비교어로 쓴다
    # (예: '미국 나스닥100 지수' → '나스닥100'). 뗀 결과가 숫자·기호뿐이거나 2글자 미만이면 쓰지 않는다.
    relevance_country_prefixes: tuple[str, ...] = ("미국", "한국", "중국", "일본", "인도", "베트남", "유럽", "글로벌")
    relevance_suffixes: tuple[str, ...] = ("지수",)
    # 구성 종목에 미국 종목이 있는 ETF는 이 시장 단어가 제목·요약에 있는 기사도 남긴다.
    relevance_us_market_terms: tuple[str, ...] = ("뉴욕증시", "미국증시", "월가", "나스닥", "S&P", "다우존스", "다우지수", "wall street")
    # 마감 후 판의 아침 예상 대조 보합 폭(%): 실제 등락 절댓값이 이 값 이하이면 보합이다. 보합 예상은 보합일 때만, 상승 예상은
    # 등락이 +폭을 넘을 때만, 하락 예상은 -폭 아래일 때만 맞음이다.
    flat_band_percent: float = 0.3
    # 마감 후 판의 그날 급변동 요약에 보여 주는 최대 건수. 넘으면 '외 N건'으로 줄인다.
    max_move_lines: int = 20
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
