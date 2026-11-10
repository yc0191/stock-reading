# 검증 규칙

코드나 설정을 바꾼 뒤, 또는 "동작한다"고 말하기 전에 읽습니다.

## 명령

| 대상 | 명령 (저장소 루트에서) | 비고 |
|---|---|---|
| Java (collector, market, news, notifier) | `./gradlew cleanTest test --no-build-cache` | JDK 21과 `JAVA_HOME_21` 필요. Docker 불필요 |
| 브리핑 (Python) | `services/briefing/.venv/bin/python -m pytest services/briefing` | 프로젝트 전용 venv. 시스템 파이썬을 쓰지 않음 |
| 설정 로더·지도 검사 (Python) | `services/briefing/.venv/bin/python -m pytest -c scripts/pytest.ini scripts/tests` | 브리핑 venv를 함께 씀. 별도 ini 필요 |
| 전체 실행 | `scripts/test-all.sh` | 위 셋과 실제 DB 테스트를 돌리고 기능별 결과 표를 출력. Docker와 개발용 Postgres 컨테이너 필요 |

설치와 환경 준비, 실제 DB 테스트와 `--live` 실행은 `docs/local-development.md`의 "테스트"를 따릅니다.

## 테스트 목록의 원본

어떤 기능을 어떤 테스트가 지키는지(단위·가상·실제, 켜는 조건 포함)의 원본은 테스트 지도 `docs/test-map/`입니다. 칸 쓰는 법, 지도 검사, `--changed`, 커밋 훅, CI는 `docs/test-map/README.md`에 있으며 이 문서에 옮겨 적지 않습니다.

## 변경 범위별로 실행할 것

| 바꾼 것 | 실행 |
|---|---|
| 모든 변경 | 지도 검사 (`python3 scripts/check_test_map.py`, 설정 로더 테스트에도 포함) |
| 한 서비스의 코드 | 그 서비스 테스트 (`./gradlew :services:<이름>:cleanTest :services:<이름>:test --no-build-cache`) |
| `contracts/events/` 또는 `Envelope`, `Topics` | Java 전체 + 브리핑 (계약 테스트가 서비스마다 있음) |
| `build-logic/`, `gradle/` | Java 전체 + `./gradlew bootJar` |
| `scripts/` | 설정 로더 테스트 |
| 커밋 전 | 전체 실행 (`scripts/test-all.sh`) |

## 관리 규칙

코드를 고칠 때는 다음 순서를 따릅니다.

1. `python3 scripts/check_test_map.py --changed`로 바꾼 파일이 걸린 기능과 확인할 테스트를 봅니다.
2. 바뀐 동작을 확인하는 테스트를 추가하거나 고치고, 위 표대로 돌립니다.
3. 커밋하면 지도 검사가 자동으로 돕니다(`pre-commit install` 1회 필요). 지도가 맞지 않으면 커밋이 막힙니다.
4. 커밋 전에 `scripts/test-all.sh`로 실제 DB 테스트까지 돌리고 결과 표를 확인합니다.
5. PR마다 CI가 실제 DB와 `--live`를 뺀 테스트를 돌립니다.

지도는 다음 변경 때 같은 커밋에서 함께 고칩니다. 고치는 방법은 `docs/test-map/README.md`의 "지도를 고치는 방법"입니다.

- 새 클래스·모듈, 새 기능
- 새 조건부 테스트, 건너뛰는 조건 변경
- 테스트 삭제·이름 변경, 테스트를 건너뛰게 하는 변경

새 외부 API를 붙이면 그 API만 부르는 확인 테스트를 켜는 조건(환경변수)과 함께 두고, `test-all.sh --live`에는 넣지 않습니다. 지도의 실제 칸에는 `[외부 API, 켜는 조건: ...]`을 쓰거나, 아직 없으면 `없음 - 이유`를 씁니다.

실행할 때마다 바뀌는 테스트 개수는 문서에 적지 않습니다.

## 함정

- `--no-build-cache`를 빼면 입력이 같을 때 테스트를 실행하지 않고 이전 결과를 복원합니다. "통과"가 실제 실행이 아닐 수 있습니다.
- 통과만 보고 끝내지 않고 `scripts/test-all.sh`의 기능별 결과 표에서 건너뜀(이유)과 실패를 확인합니다. 돌아야 할 실제 DB 테스트가 건너뜀이면 원인을 찾습니다.
- `.env` 값에는 ASCII만 씁니다. 스프링이 Latin-1로 읽어 한글이 깨집니다.
- 새로 클론한 저장소에는 gitignore 대상 파일(`.env`, `.venv`)이 없습니다. `scripts/setup-dev.sh`로 확인하고 만듭니다(`.env` 값은 사람이 채움).

## 테스트 작성 원칙

- 실패한 테스트를 지우거나 약하게 바꿔서 통과시키지 않습니다. 원인을 고칩니다.
- 버그를 고칠 때는 그 버그를 먼저 재현하는 테스트를 추가합니다.

## 결과 기록

- 테스트 결과는 실행한 명령과 통과·실패 개수를 함께 기록합니다.
- 환경 문제 등으로 실행하지 못한 검증은 "실행하지 못함"과 이유를 남깁니다.
