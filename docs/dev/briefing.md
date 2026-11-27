# 브리핑 서비스 (Python)

급변동(`market.move`)마다 근거를 모아 리포트를 만들어 `briefing.generated`로 내고, 수집기의 판 시작 알림(`daily.collection.completed`)이나 한도 시각에 데일리 판(아침 판, 마감 후 판)을 만들어 멤버마다 `briefing.daily.generated`로 냅니다. 흐름 전체는 `services/briefing/src/briefing/main.py`의 모듈 설명에 있습니다.

- 알림은 Kafka로 받고, 시세처리기·뉴스처리기의 자료는 내부 조회 API로 직접 묻습니다. 이유는 [ADR-006](../adr/006-microservices.md)과 [ADR-010](../adr/010-event-format.md)의 "근거 조회 규칙", 약속은 [conventions.md](../conventions.md)의 "내부 조회 API", "뉴스처리기의 저장 끝 알림", "추적 ID"에 있습니다.
- 쓰는 토픽과 파티션 수는 `main.py`의 `TOPIC_PARTITIONS`이며, 브리핑은 시작할 때 이 토픽들을 선언합니다.
- 이벤트 필드는 `contracts/events/`의 스키마가 원본입니다.

## 준비

테이블은 Flyway가 만듭니다. **브리핑을 띄우기 전에 Flyway(`briefing-migrate`)를 먼저 적용합니다.** DB에 닿지 못하거나 테이블이 없으면 브리핑은 시작하지 않습니다. 계정과 스키마는 [setup.md](setup.md)에 있습니다.

```bash
# 최초 1회: 프로젝트 전용 가상환경 (시스템 파이썬은 건드리지 않음)
python3 -m venv services/briefing/.venv
services/briefing/.venv/bin/python -m pip install -r services/briefing/requirements.txt
# 의존성이 바뀌면 위 pip install 을 다시 실행한다
# 데일리 메일 차트의 한글 글꼴을 내려받는다(git에 넣지 않음)
python3 scripts/install_font.py
```

글꼴이 없으면 경고 로그(`chart font files not found`)를 남기고 기본 글꼴로 그리므로 한글이 네모로 보일 수 있습니다.

## 실행

```bash
# 저장소 루트에서. 브리핑은 .env 파일을 읽지 않고 환경변수만 읽으므로 필요한 키만 넘긴다
# (.env 전체를 셸로 읽으면 ';'가 들어간 값이 명령으로 해석될 수 있다)
export $(grep -E '^(KAFKA_BOOTSTRAP|ANTHROPIC_API_KEY|BRIEFING_LLM_MODEL|POSTGRES_DB|BRIEFING_DB_USER|BRIEFING_DB_PASSWORD|COLLECTOR_DAILY_BRIEFING_ENABLED)=' .env | xargs)
PYTHONPATH=services/briefing/src services/briefing/.venv/bin/python -m briefing.main
```

`Ctrl+C`(또는 종료 신호)를 받으면 처리 중인 루프를 멈추고 끝납니다. 종료 때 남는 것은 아래 "재시작과 종료"를 봅니다.

## 환경변수

값이 없을 때 쓰는 기본값은 `main.py`, `llm.py`, `http_queries.py`(`DEFAULT_URLS`)가 원본입니다.

| 환경변수 | 설명 |
|---|---|
| `KAFKA_BOOTSTRAP` | Kafka 주소 |
| `ANTHROPIC_API_KEY` | LLM 키. 비어 있거나 자리표시 값이면 시작할 때 경고를 남기고 규칙 기반(`rule-based`) 본문을 만듭니다. Parameter Store의 `briefing` 그룹에서 가져옵니다([config.md](config.md)) |
| `BRIEFING_LLM_MODEL` | 리포트를 쓰는 모델 |
| `POSTGRES_DB` | 작업 기록 DB 이름 |
| `BRIEFING_DB_USER` / `BRIEFING_DB_PASSWORD` | 브리핑 전용 DB 계정(값은 `.env`에만) |
| `MARKET_API_URL` / `NEWS_API_URL` | 시세처리기·뉴스처리기 내부 조회 API 주소 |
| `BRIEFING_FONT_DIR` | 차트 글꼴 폴더(기본값은 `daily_chart.py`) |
| `COLLECTOR_DAILY_BRIEFING_ENABLED` | 수집기의 판 시작 알림과 같은 값. `true`일 때만 데일리 판의 한도 시각과 장 시작 직후 급변동의 아침 판 합침이 켜집니다(`DailySettings`의 주석) |

## 설정값

아래 값은 모두 `services/briefing/src/briefing/config.py`의 기본값이며 환경변수로 바꿀 수 없습니다. 값과 뜻, 그 값을 고른 이유는 그 파일의 주석에 있습니다.

- `CoreSettings`: 급변동 근거의 Δ·시간 범위·건수 상한, 저장 끝 알림 대기 상한, 같은 종목 변동 묶음, 재검색 상한, 같은 사건 기사 묶기(`article_group_*`, 데일리 판도 같은 값), 아침 판 개장 흐름 범위(`open_flow_*`)
- `QuerySettings`: 내부 조회 API를 다시 부르는 규칙(응답 제한(시세처리기 데일리 자료 조회만 `daily_call_timeout`으로 따로 둠), 최대 시도, 시도 사이 간격, 동시 호출 수). 급변동·데일리 판·재검색이 함께 씁니다.
- `DailySettings`: 데일리 판의 범위, 상한, 장 시작 시각과 아침 판 합침 구간, 한도 시각, 보합 폭, 차트·이벤트 크기 상한, 멈춘 작업 기준, 판의 문장 단위 재검색 요청 합계(급변동은 `CoreSettings`)
- `StoreSettings`: 작업 기록 저장을 다시 시도하는 횟수와 간격
- 수신 루프가 한 번에 읽는 메시지 수와 시간: `main.py`의 `MAX_MESSAGES_PER_RUN`, `MAX_READ_SECONDS`

## 동작 좌표

동작 설명은 코드와 그 옆 주석이 원본입니다(모두 `services/briefing/src/briefing/` 아래).

| 무엇 | 코드 |
|---|---|
| 같은 종목 변동 묶음 | `move_group.py` |
| 장 시작~아침 판 사이 급변동의 아침 판 합침 | `move_merge.py` |
| 급변동 근거 조회와 다시 부르기 | `jobs.py`, `http_queries.py` |
| 급변동 레벨 판단(상·중), 중의 대표 기사 고르기 | `job_log.py`의 `level_of`, `move_representative.py` |
| 비교 숫자, 머리말 | `move_figures.py`, `move_evidence.py` |
| 레벨 상 메일: 오늘 흐름, 글자판, HTML, 장중 차트 | `move_flow.py`, `move_view.py`, `move_mail.py`, `move_chart.py`, `move_prompts.py` |
| 같은 사건 기사 묶기 | `article_groups.py` |
| LLM 출력 검사(인용 번호, 미국 티커 거부(급변동·마감 후 판), 근거 약함 줄) | `llm_output.py` |
| 근거가 약한 문장 단위 재검색 | `sentence_research.py`, `research.py` |
| 발행 직전 마지막 검사 | `final_check.py`, `move_final_check.py`, `daily_final_check.py` |
| 데일리 판 시작, 한도 시각, 휴장 알림 | `daily_start.py` |
| 판 작업, 멤버별 판 | `daily_jobs.py`, `daily_sentences.py`(재검색 어댑터), `daily_publish.py`(메일 이벤트·차트·서버 상태 절), `daily_members.py`, `member_store.py` |
| 판 입력, 기사 관계 판정, 개장 흐름 | `daily_inputs.py`, `daily_relevance.py`, `open_flow.py` |
| 마감 후 판의 아침 예상 대조, 그날 급변동 | `daily_review.py` |
| 서버 상태 절 | `status.py` |
| 본문(글자판·HTML·차트)과 프롬프트 | `daily_sections.py`(입력·항목별 섹션), `daily_report.py`(LLM 입력·본문 글·작성기), `daily_output.py`(LLM 출력 검사), `daily_html.py`, `daily_chart.py`, `daily_prompts.py` |

- 급변동은 시세처리기가 정한 감지 유형(`detectionType`)으로 레벨을 나눕니다. `prevCloseHigh`면 상, 그 밖(`window`, `prevClose`, 감지 유형이 없는 이전 형식)은 중입니다.
  - 상은 근거 조회, 재검색, LLM, 메일 발행까지 지금의 리포트 흐름을 그대로 탑니다. 머리말과 메일 제목은 오늘 등락(전일 종가 대비)을 먼저 씁니다. 그날 첫 상은 기준가가 전일 종가라 오늘 등락만(`오늘 -7.20% 하락`), 두 번째 상부터는 기준가(앞선 상의 가격) 대비 변동률을 덧붙입니다(`오늘 -14.00% · 기준가 대비 -7.53% 하락`). 첫 상인지는 기준가 대비 변동률과 오늘 등락이 같은지로 가려 결과 이벤트의 `baseKind`(`prevClose`·`earlierMove`)로 발송기에 알립니다. 시장과 비교하는 숫자도 오늘 등락입니다. 등락률 색칠은 부호(+, −)가 붙은 %만 하고, 부호 없는 %와 `%p`(차이값)·%포인트는 기본 글자색으로 둡니다(데일리 판과 같음).
  - 중은 LLM·재검색 요청·결과 이벤트·메일이 없습니다. 저장 끝 알림(`news.stored`) 뒤에 근거 시간 범위의 기사만 한 번 조회해 대표 기사 하나를 고르고, `job_log`에 `representative-article` 줄로 남긴 채 `succeeded`로 끝납니다. 대표 기사는 원인이 아니라 그 변동 때 가장 눈에 띈 기사입니다.
  - 대표 기사 규칙(`move_representative.py`): 같은 사건 묶음이 있으면 기사가 가장 많은 묶음에서 변동 시각에 가장 가까운 기사, 묶음이 없으면 변동 직전에 발행된 가장 최근 기사, 고를 기사가 없으면 관련 기사 없음(`data.article`이 `null`)입니다. `data.basis`에 `group`, `latest-before`, `no-article`로 근거를 남깁니다.
  - 상 메일은 짧습니다(`report.py`, `move_view.py`). 원인은 3개까지(`MAX_CAUSES`, 넘으면 출력을 거부), 원인마다 제목과 구체 문장·뜻 문장 한 문장씩이고 한 칸에 두 문장이면 거부합니다. 기사끼리 수치·해석이 엇갈리면 LLM이 선택 칸 `conflict`(한 문장)와 `conflictRefs`를 내고 코드가 `시각 차이: …` 한 줄로 붙이며, 이 줄도 코드가 붙인 근거 번호만 인용합니다. 근거 목록은 원인마다 되풀이하지 않고 맨 아래 한 번(`근거:`)만 둔 뒤 면책이 옵니다. 원인마다 붙던 '근거 약함: …' 줄도 상 메일 글자판·HTML에는 싣지 않습니다(부족한 점은 `missing-items` 줄의 `weakItems`에 남고 재검색·다시 쓰기는 그대로입니다. 데일리 판 표시는 그대로). 원인 칸(`detail`·`meaning`·`conflict`)은 공백 포함 60자 이내여야 하며(`CoreSettings.max_cause_chars`, 근거 번호 표시는 코드가 붙이므로 세지 않고 제목은 제외) 넘으면 출력을 거부해 다시 부릅니다. 프롬프트(`move_prompts.system_prompt`)는 같은 상한을 설정에서 채워 "칸마다 짧게, 한 가지 사실만" 쓰게 하고, 자료에서 확인되지 않는 것은 원인 문장에 쓰지 말고 빼게 합니다. 데일리 판 프롬프트는 그대로입니다. '확인하지 못한 것'은 메일 글자판·HTML에 싣지 않고 `job_log`의 `missing-items` 줄(`data.missingItems`)에만 남깁니다. 차트를 못 그린 사유도 거기에 남습니다.
  - '오늘 흐름'(`move_flow.py`)은 그날(한국 날짜) 같은 종목의 앞선 급변동(중·상 모두)을 작업 기록(`job_run`)에서 읽어 시각순 목록으로 만듭니다. 줄은 `시:분 오늘 등락률 (상, 메일 보냄|중, 기록만)`이고 앞선 변동이 없으면 절을 만들지 않습니다. 같은 값이 LLM 입력의 `todayFlow`로도 들어갑니다(근거 번호가 없어 인용하지 않는 참고 자료). 작업 기록을 읽지 못하면 비워 두고 메일은 나갑니다.
  - 장중 차트(`move_chart.py`, PNG)는 그날 `chart_start`(09:00)부터 변동 5분 뒤까지 `chart_interval`(1분) 간격 가격을 선으로 그리고, 전일 종가를 점선으로, 앞선 급변동과 이 변동을 세로선으로 표시합니다. 가격은 시세처리기의 기존 시세 이력 조회를 구간·간격만 바꿔 한 번 더 부릅니다(`market.chart-history` 단계, 최대 `max_chart_points`칸). 전일 종가는 현재가와 오늘 등락률에서 거꾸로 구한 근사값이며(전일 종가 대비 유형은 이벤트의 기준가), 코스피 비교선은 없습니다. 이 조회가 끝내 실패해도 작업은 실패하지 않고 그림만 빠집니다.
  - `briefing.generated`에는 글자판(`text`)과 함께 선택 칸 `html`과 `images`(그림 한 장, `contentId` `chart-move`, base64 PNG)를 싣습니다. HTML의 `cid:`와 `images`의 `contentId`는 남김없이 서로 맞습니다. 그림이 한 장 상한(`max_mail_image_bytes` 300KiB)이나 이벤트 상한(`max_mail_event_bytes` 900KB, 데일리 판과 같은 값)을 넘으면 그림만 빼고 글자판+HTML로 보냅니다. 상한은 브리핑 생산자의 Kafka 메시지 한도(librdkafka 기본 약 1MB, `message.max.bytes`는 바꾸지 않음) 안입니다.
  - 마감 후 판의 '그날 급변동' 줄은 `시각 종목 오늘 등락 · 결과`입니다. 상은 `메일 보냄`(리포트가 실패했거나 근거가 없으면 `리포트 실패`, `근거 없어 보내지 않음`), 중은 `대표 기사: 제목 (언론사, 월-일 시:분)` 또는 `관련 기사 없음`입니다. 장 시작 직후 아침 판에 합친 변동은 레벨과 무관하게 `아침 판에 합침`입니다.
- 서버 상태의 급변동 줄은 성공으로 끝난 작업을 레벨로 나눠 `상 N(메일) · 중 M(기록)`으로 보입니다. 상은 결과 이벤트를 낸 작업, 중은 대표 기사만 기록한 작업이며, 레벨은 `job_run` 입력의 감지 유형으로 정합니다(`job_log.py`의 `level_of_input`). 실패, 진행 중, 아침 판에 합침, 근거 없어 보내지 않음은 레벨과 무관하게 따로 셉니다. 멈춤 설정이 있던 때의 지난 기록(`failure_reason=move_report_paused`)은 상·중에 넣지 않고 `지난 리포트 멈춤 N`으로 따로 보입니다.
- 인용한 근거가 없는 급변동 리포트는 발행하지 않습니다(`job_run`은 `succeeded`, 사유 `no_evidence`).
- 데일리 판은 자료나 LLM이 끝내 실패해도 있는 자료로 나갑니다. 받을 멤버를 읽지 못하면 `members_unreadable` 실패입니다.
- 작업 끝 기록(`job_run` 고침)이 끝내 실패하면 결과 이벤트를 발행하지 않습니다.
- 휴장일 판단은 수집기에만 둡니다(`CLAUDE.md` 설계 규칙, [ADR-012](../adr/012-collector-timers-and-command-topic.md)).
- 화면 로그에는 종목코드, 티커, 검색어를 쓰지 않습니다.

## 테스트

```bash
# 실제 Kafka·DB·LLM을 호출하지 않음
services/briefing/.venv/bin/python -m pytest services/briefing
```

실제 DB를 쓰는 전체 실행과 실제 외부 호출은 [testing.md](testing.md), 기능별 테스트 목록은 [테스트 지도](../test-map/briefing.md)에 있습니다.

## 손으로 확인

급변동 흐름은 `scripts/manual-briefing-move.sh [종목]`으로 봅니다. 로컬 Kafka에 가짜 `market.move`와 그 변동에 쓸 수 있는 `collection.completed`를 한 건씩 넣고, 뉴스처리기가 낸 `news.stored`와 `briefing.generated`를 보여 줍니다. 시세·기사 조회는 내부 조회 API라 `briefing.job_step`에서 봅니다. 미리 인프라, 브리핑, 시세처리기, 뉴스처리기를 띄워 둡니다.

데일리 판은 `scripts/manual-daily-briefing.sh`로 끝까지 봅니다. 개발용 Kafka·DB를 쓰지 않고 일회용 Kafka와 개발용 Postgres 안의 새 데이터베이스, 임시 폴더에서 서비스를 띄운 뒤 그것만 지웁니다. 실제 시세·뉴스 검색 API는 부르지 않습니다.

```bash
JAVA_HOME_21=<JDK 21 경로> scripts/manual-daily-briefing.sh                         # 가짜 LLM(로컬 가짜 서버)
JAVA_HOME_21=<JDK 21 경로> DAILY_E2E_REAL_LLM=1 ANTHROPIC_API_KEY=... scripts/manual-daily-briefing.sh   # 실제 Claude
JAVA_HOME_21=<JDK 21 경로> DAILY_E2E_SMTP_SINK=1 scripts/manual-daily-briefing.sh   # 127.0.0.1 가짜 SMTP 서버로만 보내고 .eml 저장
```

- 기본 모드는 루프백의 가짜 Claude 서버를 `ANTHROPIC_BASE_URL`로 지정해 실제 호출이 없습니다. 키는 출력하지 않습니다.
- 서비스 DB 계정은 `.env`의 `MARKET_/NEWS_/BRIEFING_DB_USER·PASSWORD`를 읽습니다. 다른 파일은 `DAILY_E2E_ENV_FILE`로 줍니다.
- 판 본문은 `DAILY_E2E_OUT_DIR`(기본 임시 폴더, 저장소 안 경로는 거절)에 씁니다. 결과에는 Claude 호출 수와 드라이런 메일마다 제목·HTML 글자 수·그림 수·크기가 나옵니다.

## 작업 기록 보기

급변동, 데일리 판, 재검색은 `briefing` 스키마의 테이블에 같은 형식으로 남습니다.

| 테이블 | 내용 |
|---|---|
| `job_run` | 작업마다 한 줄. `result_event_id`가 메일의 "브리핑 ID" |
| `job_log` | 일마다 한 줄 |
| `job_step` | 내부 조회와 LLM 호출 시도마다 한 줄 |
| `collection_completed` | 수집 완료 알림 |
| `market_closed_edition` | 휴장 알림을 받은 판 |

- 칸은 `services/briefing/db/migration/`의 번호 파일이 원본입니다.
- 작업 종류, 상태, 작성 방식, 기록 종류(`entry_type`)의 값은 `job_log.py`의 enum(`JobKind`, `RunStatus`, `GeneratedBy`, `EntryType`)이 원본입니다. 새 값을 더하면 enum과 새 번호 파일을 함께 고칩니다(어긋나면 `test_job_log_schema.py`가 실패).

작업 하나의 기록은 브리핑 ID나 작업 id로 봅니다. 이 스크립트는 읽기만 하며, 출력에 검색어와 종목이 있으므로 화면 로그나 공개 문서에 붙여 넣지 않습니다.

```bash
JOB_HISTORY_DB_URL=postgresql://localhost:5432/stock JOB_HISTORY_DB_USER=<읽기 계정> \
  JOB_HISTORY_DB_PASSWORD=<비밀번호> python3 scripts/job_history.py <브리핑 ID 또는 job_id> [--full]
```

```sql
-- 메일 하나의 호출 기록(5개 서비스 로그도 같은 추적 ID로 찾음)
SELECT * FROM briefing.job_step WHERE correlation_id = '<추적 ID>' ORDER BY id;
```

- 끝내 실패한 작업은 `job_run.status = 'failed'`, 사유는 `failure_reason`, 단계가 실패한 줄은 `entry_type = 'step-failed'`입니다.
- 급변동 실패 사유는 `ports.py`의 `FailureReason`입니다. `collection_not_covered`는 저장 끝 알림을 대기 상한 안에 받지 못한 것, `*_no_response`는 그 자료를 최대 시도 안에 받지 못한 것, `llm_error`는 LLM 출력이 시도 상한까지 검사를 통과하지 못했거나 호출이 실패한 것입니다.
- `interrupted_by_restart`는 모든 작업 종류에(`job_log.py`), `result_handling_error`는 조회 결과를 작업에 넘기다 예상하지 못한 오류가 난 작업에(`main.py`의 `deliver_results`, 어느 조회였는지는 그 `step-failed` 줄의 `data.queryKind`), `members_unreadable`은 데일리 판에(`daily_jobs.py`) 붙습니다.
- 성공으로 끝났지만 리포트가 없으면 `failure_reason`에 `no_evidence`, `merged_into_morning`이 남습니다(`job_log.py`의 `REASON_*`). 레벨 중은 사유 없이 성공으로 끝나고 `representative-article` 줄이 남습니다.
- `representative-article` 줄을 쓰려면 Flyway `V6__add_representative_article_entry_type.sql`이 먼저 적용돼 있어야 합니다(`job_log`의 기록 종류 값 제한).

## 재시작과 종료

- 작업 상태는 메모리에만 둡니다. 시작할 때 메시지를 받기 전에 이전 실행에서 `running`으로 남은 작업을 `failed`·`interrupted_by_restart`로 끝내며(`job_log_store.py`의 `fail_unfinished`), 리포트는 다시 만들지 않습니다.
- `market.move`는 읽는 즉시 오프셋(읽은 위치)을 확정합니다(이유는 `main.py`의 `handle_message` 주석).
  - 정상 종료: 모으는 중이던 변동은 `interrupted_by_restart` 실패 작업으로 남습니다(`main.py`의 `close`).
  - 강제 종료(`kill -9`, 전원 꺼짐): 모으는 중이던 변동은 기록 없이 사라집니다.
- 작업 기록 보관 기간은 아직 정하지 않았습니다([README.md](README.md)의 "아직 정하지 않은 것").
