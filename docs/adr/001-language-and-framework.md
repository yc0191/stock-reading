# ADR-001: 메인 서비스는 Java 21 + Spring Boot, 브리핑 AI는 Python

- 상태: **확정**
- 기록일: 2026-10-20

## 맥락
기준 1(되돌리기 어려운 선택): 언어는 서비스를 다시 짜지 않고는 바꿀 수 없다. 이 프로젝트가 집중할 것은 언어가 아니라 이벤트 구동 MSA와 근거 인용 LLM이다.

## 고려한 대안
Kotlin + Spring Boot, Go, Node.js(NestJS)

## 결정
- 메인 서비스: **Java 21 + Spring Boot** (Gradle)
- 브리핑 AI(LLM): **Python**

## 이유
1. 익숙한 스택을 써서 노력을 Kafka 설계와 배포에 쓴다.
2. `spring-kafka`, Testcontainers 등 연동 생태계가 성숙해 있다.
3. Java 21 가상 스레드로 외부 API 호출 같은 I/O 대기를 단순하게 처리한다.
4. LLM·평가 도구 생태계 때문에 브리핑은 Python으로 나누고, 언어 경계가 서비스 경계가 된다(ADR-006).

## 감수하는 점
- JVM 서비스는 메모리를 많이 쓴다. 컨테이너마다 메모리 상한을 두고 실측으로 정한다(`infra/deploy/README.md`).
- 언어가 2개가 되는 운영 비용을 감수한다.
