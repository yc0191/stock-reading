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
    # 시세 이력은 T−5분~T+Δ 구간을 이 간격으로 솎아 받는다. 기본값으로 10분 구간이 60점에 딱 맞는다.
    history_interval: timedelta = timedelta(seconds=10)
    max_history_points: int = 60
    max_articles: int = 50
    # 공시는 하루 한 번(장 시작 전) 받으므로 +Δ를 두지 않는다. 미국 금요일 장 마감 뒤 공시를 월요일 변동에서 놓치지 않게 72시간을 본다.
    filings_window: timedelta = timedelta(hours=72)
    max_filings: int = 20
    # 배경 자료(환율·지수·시장종합·일봉)와 해외 등락률은 받은 시각이 이 기간보다 오래되면 없음으로 본다.
    background_window: timedelta = timedelta(days=4)
    max_background_bytes: int = 32 * 1024
    max_tickers: int = 100
    # 같은 eventId 중복 처리를 위해 기억하는 작업 키 수. 메모리가 무한히 늘지 않게 한다.
    remembered_jobs: int = 10000


@dataclass(frozen=True)
class StoreSettings:
    # 실패 기록 저장은 수신 루프 안에서 불리므로, 다시 시도해도 루프가 오래 멈추지 않게 간격을 짧게 둔다.
    record_attempts: int = 3
    record_retry_interval: timedelta = timedelta(milliseconds=500)


DEFAULT_SETTINGS = CoreSettings()
DEFAULT_STORE_SETTINGS = StoreSettings()
