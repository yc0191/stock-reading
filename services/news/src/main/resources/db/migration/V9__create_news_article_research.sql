-- 기사와 그 기사를 찾은 재검색 작업(news.research.requested)을 다대다로 잇는다. 브리핑은 문장·회차마다 새 작업 id 로
-- 재검색하므로 앞선 작업이 이미 찾은 기사를 뒤의 작업이 다시 찾는 일이 흔하다. news_article.research_job_id 는 처음 찾은
-- 작업 하나만 남겨, 뒤의 작업으로 조회하면 그 기사가 빠졌다. 그 칸은 지우지 않고 두되 조회는 이 표로 한다.
CREATE TABLE news_article_research (
    article_id      BIGINT      NOT NULL REFERENCES news_article (id) ON DELETE CASCADE,
    research_job_id UUID        NOT NULL,
    found_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (article_id, research_job_id)
);

-- 브리핑이 재검색 작업 id 로 그 작업의 기사만 다시 조회한다.
CREATE INDEX idx_news_article_research_job_article ON news_article_research (research_job_id, article_id);

-- 이미 남은 연결(처음 찾은 작업)을 옮긴다. 받은 시각을 찾은 시각으로 삼는다.
INSERT INTO news_article_research (article_id, research_job_id, found_at)
SELECT id, research_job_id, received_at
FROM news_article
WHERE research_job_id IS NOT NULL;
