# 로컬 개발

로컬에서 서비스를 띄우고 확인하는 방법입니다. 설정값과 기본값의 원본은 각 서비스의 `application.yml`과 브리핑 `config.py`, 이벤트 필드의 원본은 `contracts/events/`이며, 이 폴더의 문서는 그 위치를 가리킵니다.

| 하려는 일 | 볼 곳 |
|---|---|
| 처음 준비(JDK, Docker, `.env`, venv), 빌드, 서비스 실행 | [setup.md](setup.md)의 "준비" |
| DB 계정, Flyway, 업무 데이터(종목·멤버·포트폴리오) 넣기 | [setup.md](setup.md) |
| Parameter Store에서 `.env`로 설정 가져오기 | [config.md](config.md) |
| 수집기의 외부 데이터(시세·뉴스·공시·구성 종목) 설정, 수동 실행, 시세 API 실서버 확인 | [collector-sources.md](collector-sources.md) |
| 시세처리기, 뉴스처리기, 조치 판정 확인기, 이메일 발송기 | [services.md](services.md) |
| 브리핑 서비스 실행, 작업 기록 보기 | [briefing.md](briefing.md) |
| 전체 테스트, 실제 DB 테스트, `--live`, 실데이터 재생 | [testing.md](testing.md) |
| 서비스 간 약속(내부 조회 API, 저장 끝 알림, 추적 ID) | [../conventions.md](../conventions.md) |
| 기능별 테스트 목록과 켜는 조건 | [../test-map/README.md](../test-map/README.md) |
| 설계 결정의 이유 | [../adr/README.md](../adr/README.md) |

## 아직 정하지 않은 것

- **작업 기록 보관 기간:** `briefing.job_run`과 `briefing.job_log`는 줄을 지우지 않고 쌓입니다. LLM 입력과 원래 응답을 그대로 담아 커지므로 보관 기간과 정리 방법을 정해야 합니다. 오래된 `job_log` 줄만 지울지, `job_run`까지 지울지가 남은 선택입니다.
- **뉴스 수집 완료 테이블:** `briefing.collection_completed`의 보관 기간과 정리 방법을 정해야 합니다.
- **휴장일 판정:** 수집기가 시세 제공자의 장 운영 상태 조회로 평일 아침(`collector.market-status.decide-cron`)에 판정합니다. 판정에 성공하면 그날 하루 동안 그 결과를 씁니다. 실패하면 영업일로 보고 잠시 뒤 다시 조회합니다. 판정 규칙은 `services/collector/.../marketdata/BrokerMarketDay.java` 주석에 있습니다. 남은 일은 실제 휴장일 응답으로 이 규칙을 확인하는 것입니다.

## 보안

- 비밀값(`.env`, 토큰, 키)과 개인 정보(보유 종목·금액·계좌)는 저장소에 올리지 않습니다.
- 커밋 전에 `gitleaks`가 검사합니다. gitleaks를 먼저 설치한 뒤 `pre-commit install`을 한 번 실행합니다(훅 설정은 [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml)). 이 검사는 로컬 커밋에서만 돌며, CI에서는 돌지 않습니다.
