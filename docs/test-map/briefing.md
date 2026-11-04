# briefing 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 근거 조회 작업(상태, 이벤트 모양, 설정값) | `__init__.py`, config.py, events.py, jobs.py, ports.py, sources.py | test_jobs.py, test_contracts.py | 해당 없음 - 메시지와 시각을 인자로 받을 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 근거 뉴스 선택(시간 범위) | evidence.py | test_evidence.py | 해당 없음 - 뉴스와 시각을 인자로 받을 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 변동을 브리핑 문장으로 바꾸기 | compose.py, service.py | test_compose.py, test_service.py | 해당 없음 - 메시지와 뉴스를 인자로 받을 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 리포트 생성(LLM, 규칙 기반 대체) | report.py | 없음 - LLM 호출과 나뉘지 않은 로직은 가짜 LLM 테스트에서 함께 확인 | test_report.py, test_scenarios.py | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| 근거 부족 시 재검색(검색어 LLM, 재검색 요청·완료 대기, 재검색 기사 조회·거르기, 다시 쓰기, 재검색 기록) | research.py | test_research.py | test_research.py, test_runner.py | 없음 - 실제 Claude·뉴스 검색으로 재검색을 거치는 확인은 아직 자동화하지 않음(작업자 승인 뒤 수동 확인) |
| Claude 호출(키·모델 판단, SDK 호출) | llm.py | test_llm.py | test_llm.py | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| Kafka 실행부(토픽 선언, 요청·응답 연결) | main.py | test_topics.py | test_runner.py | scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 실패 기록 | failure_store.py | 해당 없음 - DB 쓰기와 재시도가 전부라 가짜 DB 로 가상 칸에서 확인 | test_failure_store.py | test_failure_store_db.py [DB, 켜는 조건: BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD] |

테스트 도우미: contracts.py, fake_anthropic.py
