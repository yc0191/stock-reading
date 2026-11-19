# scripts 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 설정 로더(Parameter Store → .env) | load_config.py | test_load_config.py | test_load_config.py | 없음 - 아직 없음 [외부 API] |
| 시세 실제 호출 도구의 안전 실행 | live_probe.py | test_live_probe.py | test_live_probe.py | 해당 없음 - 실제 호출은 이 도구가 켜는 collector 의 시세 실제 테스트가 확인 |
| 가설 검증과 표 생성 | hypotheses.py | test_hypotheses.py | test_hypotheses.py | 해당 없음 - 기록 파일과 테스트 결과만 읽으며 외부 의존을 부르지 않음 |
| 사람이 로컬 흐름을 확인하는 도구 | fake-news-search.py, manual-briefing-move.sh, publish-daily-notice.sh | test_publish_daily_notice.py; 없음 - 나머지는 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 없음 - 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 해당 없음 - 도구 자체가 사람이 실제 로컬 흐름을 확인하는 수단 |
| 데일리 브리핑 끝까지 확인 | manual-daily-briefing.sh, daily_e2e.py | test_daily_e2e.py | 없음 - 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 없음 - 사람이 직접 돌리는 수동 확인이며 자동 테스트로 묶지 않음(실제 Claude 는 DAILY_E2E_REAL_LLM=1 일 때만) [서비스 흐름] |
| 전체 실행과 결과 표 | test-all.sh, personal-db-lib.sh, run_report.py, live_briefing.py | test_run_report.py, test_live_briefing.py, test_ops_db.py | test_run_report.py | test_quote_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live]; test_briefing_flow_live.py [서비스 흐름, 외부 API, 켜는 조건: test-all.sh --live]; test_ops_db.py [DB, 켜는 조건: OPS_IT_DB_URL·OPS_IT_DB_USER·OPS_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD] |
| 구성 종목 리허설 | fake_fund_site.py | test_members_flow_rehearsal.py | 해당 없음 - 리허설 자체가 가짜 운용사 서버로 실제 서비스 흐름을 돌리는 확인 | test_members_flow_rehearsal.py [서비스 흐름, 켜는 조건: MEMBERS_FLOW_DB_URL·MEMBERS_FLOW_DB_USER·MEMBERS_FLOW_DB_PASSWORD·MEMBERS_FLOW_REQUEST_LOG] |
| 브리핑 작업 하나의 기록 보기 | job_history.py | test_job_history.py | test_job_history.py | 해당 없음 - 읽기만 하는 도구이며 실제 테이블 읽기는 사람이 job_run·job_log를 쌓은 DB에서 직접 확인(job_log 저장 자체의 실제 DB 테스트는 briefing 지도에 있음) |
| 새 작업 폴더의 로컬 준비 | setup-dev.sh, install_font.py | test_setup_dev.py, test_install_font.py | test_setup_dev.py, test_install_font.py | 없음 - 실제 venv 설치와 글꼴 내려받기는 사람이 scripts/setup-dev.sh 로 확인하며 자동 테스트로 묶지 않음 [외부 API] |
| 수집기 수동 실행 명령 보내기 | collector-command.sh | test_collector_command.py | test_collector_command.py | test_quote_flow_live.py [서비스 흐름, 외부 API, 켜는 조건: test-all.sh --live] |
| 조치 판정(확인기 결과 파일 → 이슈 댓글·라벨) | ops_verdict.py | test_ops_verdict.py | test_ops_verdict.py | 없음 - 실제 GitHub·S3 호출은 워크플로가 하며 첫 운영 평일에 사람이 확인(자동 테스트로 묶지 않음) [외부 API] |
| 테스트 지도 검사, 바꾼 파일로 확인할 테스트 찾기 | check_test_map.py | test_check_test_map.py | 해당 없음 - 저장소 파일과 로컬 git 만 읽음 | 해당 없음 - 외부 의존을 부르지 않음 |

테스트 도우미: 없음
