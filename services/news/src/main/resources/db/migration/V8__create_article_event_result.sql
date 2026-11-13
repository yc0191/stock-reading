-- 기사 이벤트(news.collected)마다 처리 결과를 한 행 남긴다. 수집 완료 알림이 가리키는 기사들이 모두 처리됐는지
-- 이 표로 판단해 '저장 끝'(news.stored)을 낸다. 같은 이벤트를 다시 받아도 행은 하나다.
CREATE TABLE article_event_result (
    event_id       UUID        PRIMARY KEY,
    result         TEXT        NOT NULL CHECK (result IN ('stored', 'merged', 'failed')),
    correlation_id TEXT,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
