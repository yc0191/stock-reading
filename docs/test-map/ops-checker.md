# ops-checker 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 기준 파일 읽기(이름이 <이슈>-<시도>.yml인 파일만(YAML이 숫자로 읽은 시도도 파일 이름과 문자열로 비교), 파일 바이트의 SHA-256 지문, 형식 검사: 이슈·시도가 이름과 같음, comment 참거짓, 조치 커밋·측정일 수, 지표 key 중복 없음, 조회문 한 문장만(personal 스키마와 `;` 뒤 문장 거부), 통과식은 0~1 수와의 비교, 형식이 틀린 파일은 이유 종류만 담아 그 파일만 error, 저장소의 기준 파일이 모두 형식에 맞음과 #74 시험용 comment: false) | criteria.py | test_criteria.py | 해당 없음 - 파일만 읽고 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 지표 판정(결과는 0~1 비율 한 칸, 통과식 경계(< 와 >=), 값이 비면 hold, 모양·범위가 틀리거나 SQL 실패·10초 초과·쓰기 시도는 error와 종류만 reason) | metrics.py | test_metrics.py | test_flow.py | 해당 없음 - SQL 실행은 아래 읽기 전용 실행 행 |
| 읽기 전용 SQL 실행과 확인기 장부 쓰기(지표 SQL은 읽기 전용 트랜잭션에서 SQL 하나당 10초 상한, 끝나면 되돌림, 표를 바꾸려는 SQL은 실패, 실행 한 줄과 지표 줄을 한 트랜잭션에 쓰고 같은 event_id는 쓰지 않음) | store.py, config.py | 없음 - 가짜 연결 없이 실제 DB 테스트가 확인 | test_flow.py (가짜 장부) | test_postgres_db.py [DB, 켜는 조건: OPS_CHECKER_TEST_DB_URL·OPS_CHECKER_TEST_DB_USER·OPS_CHECKER_TEST_DB_PASSWORD, ops 마이그레이션 적용 뒤] |
| 판 시작 알림 처리(daily.collection.completed: 아침 판은 아무것도 안 함, 마감 후 판 휴장이면 계산 없이 holiday 한 줄과 checks가 빈 결과 파일, 개장일이면 기준 파일을 전부 계산해 기록, 같은 eventId는 한 번만, 확인과 쓰기 사이에 먼저 기록돼도 중복 안 남김, 한국 날짜로 날 정하기, 알림의 판 날짜가 지금 한국 날짜와 다르면 계산·기록·업로드 없이 건너뜀, 형식이 잘못된 알림은 거부하고 아무것도 남기지 않음, 오류 칸에는 종류만) | flow.py | test_flow.py | test_flow.py | 없음 - 실제 Kafka·Postgres·S3로 끝까지 도는 자동 테스트는 아직 없음 [서비스 흐름] |
| S3 결과 파일(키 results/<날짜>/post-close.json, 날·판·상태·배포 버전·기준 지문·지표 비율·판정만 담고 SQL 본문·건수·종목 없음, 올리기 실패는 실행을 error로 기록) | results.py | test_flow.py | test_flow.py | 없음 - 실제 버킷은 Terraform 적용 뒤 운영 첫 장날 사람이 확인 [외부 API] |
| Kafka 실행부(토픽 선언과 파티션 수, 알림 처리: 형식 틀린 알림만 건너뜀, 그 밖의 실패는 3번까지 다시 시도하고 포기해도 오프셋을 남김, 멈춤 요청이면 오프셋을 남기지 않음) | main.py, `__init__.py` | test_main.py (처리 루프만, 가짜 checker) | 없음 - 가짜 브로커 시험이 아직 없음 | 없음 - 실제 Kafka로 확인하는 자동 테스트는 아직 없음 [서비스 흐름] |

테스트 도우미: fakes.py
