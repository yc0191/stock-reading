-- 브리핑의 뉴스 재검색(news.research.requested)에서 찾은 기사인지와, 그 기사를 처음 찾은 재검색 작업 id 를 남긴다.
-- 작업 id 는 처음 채운 값을 유지하므로 두 번째 작업이 같은 기사를 찾아도 그 작업과의 연결은 남지 않는다.
ALTER TABLE news_article
    ADD COLUMN from_research BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN research_job_id UUID;

-- 브리핑이 재검색 작업 id 로 그 작업의 기사만 다시 조회한다.
CREATE INDEX idx_news_article_research_job_id ON news_article (research_job_id) WHERE research_job_id IS NOT NULL;
