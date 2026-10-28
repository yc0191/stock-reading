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
    max_history_points: int = 5000
    max_articles: int = 300
    # 같은 eventId 중복 처리를 위해 기억하는 작업 키 수. 메모리가 무한히 늘지 않게 한다.
    remembered_jobs: int = 10000


@dataclass(frozen=True)
class StoreSettings:
    # 실패 기록 저장은 수신 루프 안에서 불리므로, 다시 시도해도 루프가 오래 멈추지 않게 간격을 짧게 둔다.
    record_attempts: int = 3
    record_retry_interval: timedelta = timedelta(milliseconds=500)


DEFAULT_SETTINGS = CoreSettings()
DEFAULT_STORE_SETTINGS = StoreSettings()
