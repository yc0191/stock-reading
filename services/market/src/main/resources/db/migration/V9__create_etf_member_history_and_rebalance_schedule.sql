-- ETF의 상위 구성 종목 이력과 지수 교체 일정.
-- etf_member는 "지금" 구성만 담으므로 무엇이 바뀌었는지 알 수 없다. 이력은 구성이 바뀔 때만 새 행을 넣어 행 수를 작게 유지한다.

CREATE TABLE etf_member_history (
    id            BIGSERIAL PRIMARY KEY,
    etf_id        BIGINT      NOT NULL REFERENCES instrument (id) ON DELETE CASCADE,
    -- 이 시기의 상위 구성 종목 목록: [{"market","symbol","name"(선택),"weight"(선택),"rank"}]. 순위 순이다.
    -- 바뀐 것은 이 목록을 앞 행과 비교해서 찾으므로 종목별 행으로 쪼개지 않는다.
    members       JSONB       NOT NULL,
    -- 이 구성을 처음 받은 날과 마지막으로 확인한 날(한국 시간). 바뀌지 않으면 last_seen_on만 고친다.
    first_seen_on DATE        NOT NULL,
    last_seen_on  DATE        NOT NULL,
    -- 이력에 반영한 시각. 데일리 판의 시간 범위 안에 반영된 변경만 싣는 기준이다.
    applied_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (last_seen_on >= first_seen_on)
);

-- ETF마다 가장 최근 행이 현재 구성이다.
CREATE INDEX etf_member_history_etf_applied ON etf_member_history (etf_id, applied_at DESC);

-- 운용사 사이트에서 받은 응답 원문. 받을 때마다 한 행이며(실패한 응답 포함) 가공하지 않고 그대로 둔다.
-- etf_member_history의 members는 이 원문에서 주식만 골라 상위 10개로 줄인 계산 결과다.
CREATE TABLE etf_member_raw (
    id          BIGSERIAL PRIMARY KEY,
    etf_id      BIGINT      NOT NULL REFERENCES instrument (id) ON DELETE CASCADE,
    base_date   DATE        NOT NULL,
    http_status INT         NOT NULL,
    body        TEXT        NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX etf_member_raw_etf_fetched ON etf_member_raw (etf_id, fetched_at DESC);

-- 지수 교체 일정. 사람이 1년에 한 번 직접 채운다. 교체일이 든 달에는 구성 종목을 매일 확인한다.
-- tracks는 instrument.tracks와 같은 값이다(ETF가 어느 지수를 따르는지).
CREATE TABLE index_rebalance_schedule (
    id             BIGSERIAL PRIMARY KEY,
    tracks         VARCHAR(100) NOT NULL,
    effective_date DATE         NOT NULL,
    note           VARCHAR(200),
    UNIQUE (tracks, effective_date)
);
