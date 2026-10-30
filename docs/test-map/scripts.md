# scripts 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 설정 로더(Parameter Store → .env) | load_config.py | test_load_config.py | test_load_config.py | 없음 - 데일리 브리핑 작업에서 채움 [외부 API] |
| 시세 실제 호출 도구의 안전 실행 | live_probe.py | test_live_probe.py | test_live_probe.py | 해당 없음 - 실제 호출은 이 도구가 켜는 collector 의 시세 실제 테스트가 확인 |
| 가설 검증과 표 생성 | hypotheses.py | test_hypotheses.py | test_hypotheses.py | 해당 없음 - 기록 파일과 테스트 결과만 읽으며 외부 의존을 부르지 않음 |
| 사람이 로컬 흐름을 확인하는 도구 | fake-news-search.py, manual-briefing-move.sh, publish-tick.sh | 없음 - 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 없음 - 사람이 직접 돌려 결과를 눈으로 보는 도구라 자동 테스트가 없음 | 해당 없음 - 도구 자체가 사람이 실제 로컬 흐름을 확인하는 수단 |
| 전체 실행과 결과 표(단위·가상·실제 DB, --live 시세 흐름) | test-all.sh, run_report.py | test_run_report.py | test_run_report.py | test_quote_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 테스트 지도 검사 | check_test_map.py | test_check_test_map.py | 해당 없음 - 저장소 파일만 읽음 | 해당 없음 - 외부 의존을 부르지 않음 |

테스트 도우미: 없음
