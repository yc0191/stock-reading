# scripts 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 설정 로더(Parameter Store → .env) | load_config.py | test_load_config.py | test_load_config.py | 없음 - 데일리 브리핑 작업에서 채움 [외부 API] |
| 시세 실제 호출 도구의 안전 실행 | live_probe.py | test_live_probe.py | test_live_probe.py | 해당 없음 - 실제 호출은 이 도구가 켜는 collector 의 시세 실제 테스트가 확인 |
| 가설 검증과 표 생성 | hypotheses.py | test_hypotheses.py | test_hypotheses.py | 해당 없음 - 기록 파일과 테스트 결과만 읽으며 외부 의존을 부르지 않음 |
| 사람이 로컬 흐름을 확인하는 도구 | fake-news-search.py, manual-briefing-move.sh, publish-tick.sh | 없음 - 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 없음 - 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 해당 없음 - 도구 자체가 사람이 실제 로컬 흐름을 확인하는 수단 |
| 데일리 브리핑 끝까지 확인(가짜 수집 자료·시작 신호 → 시세처리기·뉴스 → 브리핑 → 가짜 또는 실제 LLM → 발송기 드라이런) | manual-daily-briefing.sh, daily_e2e.py | test_daily_e2e.py | 없음 - 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 없음 - 사람이 직접 돌리는 수동 확인이며 자동 테스트로 묶지 않음(실제 Claude 는 DAILY_E2E_REAL_LLM=1 일 때만) [서비스 흐름] |
| 전체 실행과 결과 표(단위·가상·실제 DB, --live 시세 흐름·브리핑·메일) | test-all.sh, run_report.py, live_briefing.py | test_run_report.py, test_live_briefing.py | test_run_report.py | test_quote_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live]; test_briefing_flow_live.py [서비스 흐름, 외부 API, 켜는 조건: test-all.sh --live] |
| 브리핑 작업 하나의 기록 보기(job_run 한 줄과 job_log 줄을 시간 순서로, 작업 id·메일의 브리핑 ID 모두로 찾기, 발행 안 한 성공 작업은 실패 사유 대신 발행 안 한 이유로 표시) | job_history.py | test_job_history.py | test_job_history.py | 해당 없음 - 읽기만 하는 도구이며 실제 테이블 읽기는 사람이 job_run·job_log를 쌓은 DB에서 직접 확인(job_log 저장 자체의 실제 DB 테스트는 briefing 지도에 있음) |
| 차트 한글 글꼴 내려받기(버전 고정 주소, SHA-256 검사, 이미 있으면 건너뜀) | install_font.py | test_install_font.py | test_install_font.py | 없음 - 실제 내려받기는 사람이 python3 scripts/install_font.py 로 확인하며 자동 테스트로 묶지 않음 [외부 API] |
| 새 작업 폴더의 로컬 준비(.env·JDK 21 확인, 브리핑 venv 만들기·의존성 설치, 차트 글꼴, --check 는 확인만) | setup-dev.sh | test_setup_dev.py | test_setup_dev.py | 없음 - 실제 venv 설치와 글꼴 내려받기는 사람이 scripts/setup-dev.sh 로 확인하며 자동 테스트로 묶지 않음 [외부 API] |
| 테스트 지도 검사, 바꾼 파일로 확인할 테스트 찾기 | check_test_map.py | test_check_test_map.py | 해당 없음 - 저장소 파일과 로컬 git 만 읽음 | 해당 없음 - 외부 의존을 부르지 않음 |

테스트 도우미: 없음
