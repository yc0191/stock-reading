# 검증 규칙

코드나 설정을 바꾼 뒤, 또는 "동작한다"고 말하기 전에 읽습니다.

## 명령

| 대상 | 명령 (저장소 루트에서) | 비고 |
|---|---|---|
| Java (collector, market, notifier) | `./gradlew cleanTest test --no-build-cache` | JDK 21과 `JAVA_HOME_21` 필요. Docker 불필요 |
| 브리핑 (Python) | `services/briefing/.venv/bin/python -m pytest services/briefing` | 프로젝트 전용 venv. 시스템 파이썬을 쓰지 않음 |
| 설정 로더 (Python) | `services/briefing/.venv/bin/python -m pytest -c scripts/pytest.ini scripts/tests` | 브리핑 venv를 함께 씀. 별도 ini 필요 |

설치와 환경 준비는 `docs/local-development.md`를 따릅니다.

## 변경 범위별로 실행할 것

| 바꾼 것 | 실행 |
|---|---|
| 한 서비스의 코드 | 그 서비스 테스트 (`./gradlew :services:<이름>:cleanTest :services:<이름>:test --no-build-cache`) |
| `contracts/events/` 또는 `Envelope`, `Topics` | Java 전체 + 브리핑 (계약 테스트가 서비스마다 있음) |
| `build-logic/`, `gradle/` | Java 전체 + `./gradlew bootJar` |
| `scripts/` | 설정 로더 테스트 |

## 함정

- `--no-build-cache`를 빼면 입력이 같을 때 테스트를 실행하지 않고 이전 결과를 복원합니다. "통과"가 실제 실행이 아닐 수 있습니다.
- 테스트 개수가 `docs/local-development.md`의 표보다 줄었다면 실행되지 않은 테스트가 있는지 확인합니다.
- `.env` 값에는 ASCII만 씁니다. 스프링이 Latin-1로 읽어 한글이 깨집니다.
- 새로 클론한 저장소에는 gitignore 대상 파일(`.env`, `.venv`)이 없습니다. 필요하면 새로 만듭니다.

## 테스트 작성 원칙

- 실패한 테스트를 지우거나 약하게 바꿔서 통과시키지 않습니다. 원인을 고칩니다.
- 버그를 고칠 때는 그 버그를 먼저 재현하는 테스트를 추가합니다.

## 결과 기록

- 테스트 결과는 실행한 명령과 통과·실패 개수를 함께 기록합니다.
- 환경 문제 등으로 실행하지 못한 검증은 "실행하지 못함"과 이유를 남깁니다.
