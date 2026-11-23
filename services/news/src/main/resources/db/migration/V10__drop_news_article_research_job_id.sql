-- 기사마다 처음 찾은 재검색 작업 하나만 남던 칸과 그 색인을 지운다. 기사와 재검색 작업의 연결은 V9 의
-- news_article_research 표가 작업마다 남기고(옮겨 채움도 V9 에서 끝남), 재검색 기사 조회도 그 표로 한다.
DROP INDEX IF EXISTS idx_news_article_research_job_id;
ALTER TABLE news_article DROP COLUMN research_job_id;
