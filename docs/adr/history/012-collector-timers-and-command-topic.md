# ADR-012 경위

[ADR-012](../012-collector-timers-and-command-topic.md)의 결정이 바뀐 경위입니다(날짜순). ADR 본문에는 현재 결정만 있습니다.

- 2026-11-13 변경: 구성 종목 조회는 시세처리기로 가는 tick(`collect-members`)이 아니라 수집기 타이머 작업(`collect-etf-members`)이 되었다. 수집기가 운용사 사이트에서 받고 결과는 `etf.members.fetched` 이벤트로 시세처리기에 넘긴다.
- 2026-11-13 변경: 데일리 판 시작은 `schedule.tick`(`daily-briefing-morning`·`daily-briefing-post-close`)이 아니라 수집기의 판 시작 알림(`daily.collection.completed`, 못 받은 자료 목록과 휴장 여부를 담음)으로 한다. 아침 판은 개장 흐름 구간이 끝날 때, 마감 후 판은 마감 후 수집이 끝날 때 나가고, 알림이 한도 시각까지 없으면 브리핑이 있는 자료로 시작한다. 이로써 `schedule.tick` 토픽은 쓰는 곳이 없어 선언도 없앴다(옛 계약 파일만 남김, ADR-010). 시각과 한도의 원본은 수집기 `collector.daily-briefing`과 브리핑 `DailySettings`다.
