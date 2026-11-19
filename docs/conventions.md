# 서비스 간 약속

여러 서비스가 함께 지키는 약속입니다. 이벤트 칸의 원본은 `contracts/events/`의 스키마이고, 이 문서는 칸 이름을 옮겨 적지 않고 그 스키마를 가리킵니다.

## 내부 조회 API

시세처리기와 뉴스처리기는 브리핑이 부르는 조회 API를 웹 서버로 엽니다. 브리핑은 근거 자료를 Kafka 요청·응답 대신 이 API로 직접 묻고, 알림은 계속 Kafka로 받습니다([ADR-006](adr/006-microservices.md)). 요청·응답 본문의 모양은 `contracts/events/`에 남겨 둔 옛 요청·응답 이벤트의 `payload`와 같습니다. 이 이벤트들은 토픽을 없앴고 스키마와 샘플만 '더 이상 쓰지 않음'으로 남아 있습니다. 계산은 각 조회 클래스(`HistoryQuery`, `ContextQuery`, `DailyQuery`, `ArticlesQuery`, `FilingsQuery`)가 합니다. 브리핑이 다시 부르는 규칙과 호출 기록(`job_step`)은 [브리핑 서비스](dev/briefing.md)에 있습니다.

| 서비스 | 포트 | 경로 (POST) | 본문 모양(계약 이름) |
|---|---|---|---|
| 시세처리기 | `services/market` `application.yml`의 `server.port` | `/internal/market/context` | `market.context` 요청·응답 v1 |
| | | `/internal/market/daily` | `market.daily` 요청 v1·응답 v2 |
| | | `/internal/market/history` | `market.history` 요청·응답 v1 |
| 뉴스처리기 | `services/news` `application.yml`의 `server.port` | `/internal/news/articles` | `news.articles` 요청·응답 v1 |
| | | `/internal/news/filings` | `news.filings` 요청·응답 v1 |

- **응답 코드:** 잘못된 요청은 400이고 브리핑은 다시 부르지 않습니다. 잘못된 요청은 필수 칸 누락, 상한·순번이 0 이하, 시작이 끝보다 늦은 기간, 읽을 수 없는 시각·재검색 작업 id, 티커 없는 공시 요청입니다. DB 오류 같은 서버 쪽 실패는 500이며, 브리핑은 다시 부릅니다.
- **서버 밖으로 열지 않습니다**([ADR-007](adr/007-outbound-only.md)).
  - 로컬에서는 `server.address`(각 서비스 `application.yml`)로 이 기계 안에서만 받습니다. 포트를 어떻게 골랐는지는 같은 파일 주석에 있습니다.
  - 운영 Compose(`infra/deploy/compose/services.yml`)에서는 두 서비스에 `SERVER_ADDRESS`를 주어 Compose 네트워크에서만 받고, `ports`는 두지 않습니다.
  - 브리핑은 같은 파일의 `MARKET_API_URL`·`NEWS_API_URL` 주소로 조회합니다.
- **요청 스레드 수:** `server.tomcat.threads.max`로 줄여 둡니다. 줄인 이유는 `application.yml` 주석에 있습니다. 브리핑의 동시 호출 수(`services/briefing/src/briefing/config.py`의 `QuerySettings.max_workers`)는 이 수를 넘지 않게 둡니다.

## 뉴스처리기의 저장 끝 알림

뉴스처리기는 기사 이벤트(`news.collected`)마다 처리 결과를 `news.article_event_result`에 한 줄 남깁니다. 같은 이벤트를 다시 받아도 한 줄입니다. 결과 값은 `ArticleResult`에 있습니다(새로 저장, URL 중복이라 종목·검색어만 합침, 재시도 소진으로 저장 실패).

수집 완료(`collection.completed`)를 받으면 그 `articleEventIds`가 모두 처리될 때까지 기다렸다가 저장 끝(`news.stored`)을 한 건 냅니다. 대기 한도(`services/news` `application.yml`의 `news.stored.wait-limit`)까지 처리되지 않은 기사가 있으면, 그 `eventId`를 담아 한도에서 냅니다. `articleEventIds`가 없는 수집 완료에는 내지 않습니다. 알림의 칸은 `contracts/events/news.stored.v1.schema.json`에 있습니다.

- **다시 낼 수 있습니다:** 같은 수집 완료를 다시 받으면(재시작 뒤 다시 읽기, 전송 실패 뒤 재시도) 저장 끝을 다시 냅니다. `eventId`는 수집 완료의 `eventId`로 정해지므로 다시 내도 같습니다. 받는 쪽은 `eventId`로 한 번만 처리합니다. 두 번째 알림의 개수는 첫 알림과 다를 수 있습니다.
- **기다리지 않는 경우:** 수집 끝 시각에서 `news.stored.stale-after`(비우면 대기 한도와 같음, `StoredProperties`) 이상 지난 수집 완료는 기다리지 않습니다. 이미 한 번 기다린 수집 완료를 다시 처리할 때도 기다리지 않습니다. 기다린 수집 완료는 메모리에 `StoredNotifier.WAITED_CAPACITY`건까지 기억하며, 재시작하면 잊습니다.
- **정정 알림은 없습니다:** 이때 아직 저장되지 않은 기사는 `unprocessedEventIds`에 담기고, 뒤늦게 저장돼도 정정 알림은 내지 않습니다.
- **한 번 처리의 최악 시간:** 대기 한도와 Kafka 전송 한도를 더한 값이 poll 간격 상한보다 짧아야 합니다. 셈은 `StoredNotifier` 주석에 있습니다. 대기 한도를 늘리면 이 합을 다시 확인합니다.

## 추적 ID

메일 하나를 만든 모든 서비스의 로그를 이벤트 본문의 `correlationId` 하나로 찾습니다. 이름의 원본은 이 절이며, 각 서비스는 상수로 복제합니다. Java는 각 서비스의 `trace.CorrelationId`, 브리핑은 `services/briefing/src/briefing/trace.py`입니다. 값은 언제나 이벤트 본문의 `correlationId`와 같고, 별도 추적 도구는 쓰지 않습니다.

| 쓰는 곳 | 이름 | 붙이는 방법 |
|---|---|---|
| Kafka 메시지 헤더 | `correlationId` | 보내는 모든 메시지에 본문 값을 싣습니다. Java는 `CorrelationIdProducerInterceptor`(`spring.kafka.producer.properties.interceptor.classes`)로, 브리핑은 발행 함수 한 곳에서 |
| 로그 문맥 | `correlationId` | 메시지를 처리하는 동안 찍히는 줄마다 붙습니다. Java는 `CorrelationIdRecordInterceptor`가 MDC에 넣고 `logging.pattern.correlation`이 찍습니다. 브리핑은 `trace.py`의 로그 필터와 `LOG_FORMAT`을 씁니다. 헤더가 없으면 본문에서 읽습니다 |
| 서비스 간 HTTP 요청 헤더 | `X-Correlation-Id` | 브리핑이 내부 조회 API를 부를 때 그 작업의 값을 싣습니다(다시 부를 때도 같은 값). 받는 쪽은 `CorrelationIdHttpFilter`로 로그 문맥에 넣습니다 |
| 브리핑 호출 기록 | `briefing.job_step.correlation_id` | 조회 시도와 LLM 호출마다 같은 값을 남깁니다([브리핑 서비스](dev/briefing.md)) |

메시지 처리 밖에서 도는 일도 그 일의 ID를 로그 줄에 붙입니다.
- 수집기 수집 작업에는 회차의 `correlationId`(`<jobType>-<기준 시각>`)를 붙입니다.
- 브리핑에서는 제한 시각 처리(저장 끝 대기 초과, 같은 종목 변동 묶음 시작), 끝난 조회의 결과 처리, 조회 작업 스레드의 줄에 그 작업의 `correlationId`를 붙입니다.
- 어느 일에도 속하지 않는 줄은 빈 `[]`로 찍힙니다.
