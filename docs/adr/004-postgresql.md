# ADR-004: PostgreSQL + pgvector, 서비스별 스키마와 계정

- 상태: **확정**
- 기록일: 2026-10-20

## 맥락
시세·뉴스·알림 규칙 같은 관계형 데이터와 RAG용 벡터 검색이 모두 필요하고, 서버 2대에서 컨테이너를 늘리고 싶지 않다.

## 고려한 대안
MySQL, MongoDB, OpenSearch/Elasticsearch, TimescaleDB, DynamoDB/Cassandra

## 결정
**PostgreSQL + pgvector**. 한 인스턴스 안에서 서비스별로 **스키마와 DB 계정**을 나누고, 서비스는 자기 스키마만 접근한다. 다른 서비스의 데이터는 DB를 직접 읽지 않고 Kafka 이벤트로 받는다.

## 이유 (모두 문서로 확인)
1. **벡터 검색을 같은 DB에서 해결:** pgvector는 HNSW·IVFFlat 인덱스와 코사인 등 거리 계산을 지원한다. 별도 벡터 DB 컨테이너가 필요 없다.
2. **MySQL은 현재 이 용도로 부적합:** `VECTOR` 타입은 있으나 `DISTANCE()`는 HeatWave 전용이고 벡터 컬럼에는 인덱스를 걸 수 없다(검색 결과 기준, 버전에 따라 변할 수 있음 → 도입 시 재확인).
3. **서비스별 분리가 자연스럽다:** PostgreSQL의 스키마는 DB 안의 이름 공간이다. MySQL에서는 스키마와 DB가 동의어라 DB를 여러 개 만들어야 한다.
4. Kafka, Redis, PostgreSQL 3개만으로 전체 기능이 해결된다.

## 비교 요약
- MongoDB: Community Edition에서 Search/Vector Search가 GA(2026-06-30)되었으나 별도 `mongot` 프로세스가 필요하고 GA 직후라 성숙도 미확인 → 유력한 대안으로 남김.
- OpenSearch: 키워드+벡터 하이브리드 검색에 강하나 메인 DB로는 부적합, RAG 정확도 개선 단계에서 비교 실험 대상.
- TimescaleDB, DynamoDB/Cassandra: 현재 데이터 규모에 과함.

## 주장하지 않는 것
"PostgreSQL이 MySQL보다 전반적으로 우월하다"는 주장은 하지 않는다. 이 프로젝트의 요구(벡터 + 서비스별 분리 + 적은 컨테이너)에 맞아서 선택했다.

## 감수하는 점과 완화
- 벡터 인덱스(HNSW)는 메모리를 더 쓴다 → 실측 후 IVFFlat 검토.
- 벡터 규모가 커지면 한계 → 벡터 저장소를 인터페이스 뒤에 숨겨 교체 가능하게 설계.

## 구현 메모
- 서비스 전용 계정·스키마는 `infra/compose/postgres/init/01-roles.sql`이 최초 기동 때 만든다(`\getenv`로 환경변수를 읽어 비밀번호를 SQL 텍스트에 넣지 않음).
- 시세 테이블은 날짜별 파티션이다: [ADR-018](018-quote-storage.md).
