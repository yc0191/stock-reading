-- 구성 종목 원문은 수집기가 보낸 이벤트(etf.members.fetched)로 들어온다. 같은 이벤트가 다시 와도 한 번만 저장·반영하도록
-- 이벤트 번호를 둔다. 이 변경 전에 시세처리기가 직접 받아 둔 행은 이벤트 번호가 없어 비워 둔다(유일 제약은 빈 값끼리 겹쳐도 된다).
ALTER TABLE etf_stock_raw ADD COLUMN event_id UUID;
CREATE UNIQUE INDEX etf_stock_raw_event_id ON etf_stock_raw (event_id);
