# ops-checker 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 기준 파일 읽기와 형식 검사 | criteria.py | test_criteria.py | 해당 없음 - 파일만 읽고 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 지표 판정 | metrics.py | test_metrics.py | test_flow.py | 해당 없음 - SQL 실행은 아래 읽기 전용 실행 행 |
| 읽기 전용 SQL 실행과 확인기 장부 쓰기 | store.py, config.py | 없음 - 가짜 연결 없이 실제 DB 테스트가 확인 | test_flow.py (가짜 장부) | test_postgres_db.py [DB, 켜는 조건: OPS_IT_DB_URL·OPS_IT_DB_USER·OPS_IT_DB_PASSWORD, ops 마이그레이션 적용 뒤] |
| 판 시작 알림 처리 | flow.py | test_flow.py | test_flow.py | 없음 - 실제 Kafka·Postgres·S3로 끝까지 도는 자동 테스트는 아직 없음 [서비스 흐름] |
| S3 결과 파일 | results.py | test_flow.py | test_flow.py | 없음 - 실제 버킷은 Terraform 적용 뒤 운영 첫 장날 사람이 확인 [외부 API] |
| Kafka 실행부 | main.py, `__init__.py` | test_main.py | 없음 - 가짜 브로커 시험이 아직 없음 | 없음 - 실제 Kafka로 확인하는 자동 테스트는 아직 없음 [서비스 흐름] |

테스트 도우미: fakes.py
