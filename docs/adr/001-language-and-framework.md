# ADR-001: 메인 서비스는 Java 21 + Spring Boot, 브리핑 AI는 Python

- 상태: **확정**
- 기록일: 2026-10-20

## 맥락
이 프로젝트의 목적은 Kafka, Redis 기반 MSA 경험이며 언어 자체를 학습하는 것이 아니다.

## 고려한 대안
Kotlin + Spring Boot, Go, Node.js(NestJS)

## 결정
- 메인 서비스: **Java 21 + Spring Boot** (Gradle)
- 브리핑 AI(RAG/LLM): **Python**

## 이유
1. 학습 목표를 Kafka, Redis, 배포에 집중시키기 위해 익숙한 스택을 택한다.
2. `spring-kafka`, `spring-data-redis`, Testcontainers 등 연동 생태계가 성숙해 있다.
3. Java 21 가상 스레드로 외부 API 폴링, LLM 호출 같은 I/O 대기를 단순하게 처리할 수 있다.
4. LLM/임베딩/평가 도구 생태계 때문에 RAG 서비스는 Python으로 분리하고, 언어 경계가 자연스러운 서비스 분리 이유가 된다(ADR-006).

## 감수하는 점과 완화
- JVM 서비스는 메모리를 많이 쓴다. 서버 2대 제약에서는 서비스를 합쳐 시작하고 실측 후 분리 여부를 정한다.
- 언어가 2개가 되는 운영 비용을 감수한다.

## 후속
- RAG 세부(임베딩 모델, 청킹, 검색 방식, 평가)는 구현 단계로 이월했다.
