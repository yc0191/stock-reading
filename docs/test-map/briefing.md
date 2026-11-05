# briefing 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 근거 조회 작업(상태, 이벤트 모양, 설정값) | `__init__.py`, config.py, events.py, jobs.py, ports.py, sources.py | test_jobs.py, test_contracts.py, test_sources.py | 해당 없음 - 메시지와 시각을 인자로 받을 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 근거 뉴스 선택(시간 범위) | evidence.py | test_evidence.py | 해당 없음 - 뉴스와 시각을 인자로 받을 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 변동을 브리핑 문장으로 바꾸기 | compose.py, service.py | test_compose.py, test_service.py | 해당 없음 - 메시지와 뉴스를 인자로 받을 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 급변동 배경 숫자와 비교 숫자(환율·지수 꺼내기, 시장 대비 차이, 미국 종목 집계) | move_figures.py | test_move_figures.py | 해당 없음 - 배경 원문과 등락률을 인자로 받을 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 리포트 생성(LLM, 규칙 기반 대체, 급변동 작업 기록 넘기기, 기록 저장 실패 시 미발행) | report.py | 없음 - LLM 호출과 나뉘지 않은 로직은 가짜 LLM 테스트에서 함께 확인 | test_report.py, test_scenarios.py | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| 근거 부족 시 재검색(검색어 LLM, 재검색 요청·완료 대기, 재검색 기사 조회·거르기, 다시 쓰기, 재검색 작업 기록) | research.py | test_research.py | test_research.py, test_runner.py | 없음 - 실제 Claude·뉴스 검색으로 재검색을 거치는 확인은 아직 자동화하지 않음(수동 확인) |
| Claude 호출과 출력 검사(키·모델 판단, SDK 호출, 응답 형식·근거 번호·빈 문장 검사로 급변동·데일리 공통) | llm.py, llm_output.py | test_llm.py, test_llm_output.py | test_llm.py, test_llm_output.py | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| Kafka 실행부(토픽 선언, 요청·응답 연결) | main.py | test_topics.py | test_runner.py, test_daily_runner.py | scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 데일리 판 만들기(범위 계산, 자료 모으기, 본문, 작업 기록, 발행) | daily_jobs.py, daily_report.py | test_daily_jobs.py, test_daily_report.py | test_daily_jobs.py, test_daily_runner.py | 없음 - 실제 Kafka·Postgres·Claude 끝까지 확인은 데일리 e2e-docs 구간에서 채움 [서비스 흐름] |
| 작업 기록 형식과 저장(급변동·데일리·재검색 공통 로그 테이블) | job_log.py, job_log_store.py | test_job_log_store.py | test_job_log_store.py, test_jobs.py, test_report.py, test_research.py, test_scenarios.py, test_daily_jobs.py | test_job_log_store_db.py [DB, 켜는 조건: BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD] |

테스트 도우미: contracts.py, fake_anthropic.py, fake_job_log.py
