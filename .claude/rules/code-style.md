# 코드 작성 규칙

코드를 작성하거나 수정할 때 읽습니다.

## 주석

- 새로 작성하는 주석은 한글로 작성합니다.
- 다음은 도구나 문법이 인식하므로 영어 원문을 유지합니다.
  - 도구 지시 주석 (`noqa`, `type: ignore`, `@SuppressWarnings` 등)
  - `TODO`, `FIXME` 등의 키워드 (뒤따르는 설명은 한글)
  - Javadoc·docstring의 태그명 (`@param`, `@return` 등, 태그 뒤 설명은 한글)
  - 라이선스 헤더
- 코드 식별자(변수, 함수, 클래스, 파일명)는 영어를 유지합니다.
- 주석은 "무엇"이 아닌 "왜"를 설명할 때만 작성합니다.
- 기존 파일을 수정할 때는 새로 추가하는 주석만 한글로 작성하고, 기존 주석은 요청 없이 바꾸지 않습니다.

## 구조와 관례

기존 코드를 읽고 같은 방식으로 씁니다. 새 방식을 들여오기 전에 기존 코드에 같은 문제를 푸는 선례가 있는지 찾습니다.

### Java (collector, market, notifier)

- 패키지는 `dev.stockreading.<서비스>.<기능>`입니다. 서비스끼리 코드를 import하지 않습니다(서비스 독립, ADR-006).
- 같은 패키지에서만 쓰는 클래스는 package-private으로 둡니다.
- 의존성은 생성자로 주입합니다. 필드 주입(`@Autowired`)과 Lombok은 쓰지 않습니다.
- 이벤트와 값 객체는 `record`로 씁니다. 외부 연동은 `*Port` 인터페이스 뒤에 두어 테스트에서 바꿀 수 있게 합니다.
- 로그는 SLF4J를 씁니다.
- 의존성 버전은 `gradle/libs.versions.toml`에서 관리하고, 서비스 공통 설정은 `build-logic`에 둡니다. 서비스 `build.gradle.kts`에 중복하지 않습니다.
- 테스트는 JUnit 5, AssertJ, Mockito를 쓰고, 테스트 클래스 이름은 대상 또는 검증하는 기능 이름에 `Test`를 붙입니다.

### Python (briefing, scripts)

- Kafka 통신은 `main.py`에서만 합니다. 나머지 모듈(`service.py`, `compose.py`, `evidence.py`)은 Kafka를 import하지 않고 메시지·뉴스·시각을 인자로 받습니다.
- 값 객체는 `@dataclass(frozen=True)`로 씁니다.
- 테스트는 `pytest`를 쓰고, 파일명은 `test_<대상>.py`입니다.
