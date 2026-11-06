"""브리핑 작업 하나의 기록을 시간 순서로 사람이 읽기 쉽게 보여 준다. 테이블을 읽기만 하고 고치지 않는다.

사용: python3 scripts/job_history.py <브리핑 ID 또는 job_id>

- 급변동 메일에 적힌 '브리핑 ID'(결과 이벤트의 eventId)와 작업 id(job_id) 어느 쪽을 줘도 같은 작업을 찾는다.
- briefing.job_run의 한 줄(작업 종류·상태·최종 본문·근거 약함 수·재검색 요청 수)과 그 아래 briefing.job_log의 줄을
  시간 순서로 출력한다. 문장 단위 줄은 문장 번호·회차, 검색어, 받은·쓴·버린 기사와 이유, 고치기 전·후 문장을 보여 준다.
- --full 을 주면 줄마다 관련 데이터(LLM 입력·원래 응답 포함)를 JSON 그대로 덧붙인다. 양이 많다.

연결 정보는 환경변수로 받는다(값은 출력하지 않는다).
  JOB_HISTORY_DB_URL       psycopg 주소(예: postgresql://localhost:5432/stock)
  JOB_HISTORY_DB_USER      읽기 권한이 있는 계정
  JOB_HISTORY_DB_PASSWORD  비밀번호

출력에는 검색어와 종목이 들어 있으므로 화면 로그나 공개 문서에 붙여 넣지 않는다.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any, Mapping, TextIO

RUN_COLUMNS = ("job_id", "result_event_id", "job_kind", "status", "failure_reason", "generated_by", "target",
               "started_at", "finished_at", "final_text", "weak_count", "research_count", "input")
LOG_COLUMNS = ("id", "entry_type", "stage", "sentence_no", "round", "before", "after", "reason", "error_type",
               "attempts", "data", "occurred_at")

# 브리핑 ID(결과 이벤트의 eventId)로도 찾는다. 메일에 보이는 값은 job_id에서 거꾸로 계산할 수 없다.
RUN_SQL = f"SELECT {', '.join(RUN_COLUMNS)} FROM briefing.job_run WHERE job_id = %s OR result_event_id = %s LIMIT 1"
LOG_SQL = f"SELECT {', '.join(LOG_COLUMNS)} FROM briefing.job_log WHERE job_id = %s ORDER BY occurred_at, id"


class NotFound(Exception):
    pass


def connect(env: Mapping[str, str]):
    """읽기 전용 연결. 값이 없으면 어떤 변수가 비었는지만 알린다."""
    import psycopg
    from psycopg.conninfo import make_conninfo

    missing = [name for name in ("JOB_HISTORY_DB_URL", "JOB_HISTORY_DB_USER") if not env.get(name, "").strip()]
    if missing:
        raise SystemExit(f"환경변수가 없습니다: {', '.join(missing)}")
    conn = psycopg.connect(make_conninfo(env["JOB_HISTORY_DB_URL"], user=env["JOB_HISTORY_DB_USER"],
                                         password=env.get("JOB_HISTORY_DB_PASSWORD", "")))
    conn.read_only = True
    return conn


def _rows(conn: Any, sql: str, params: tuple, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def load_history(conn: Any, key: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """job_id 또는 브리핑 ID로 작업 한 줄과 그 작업의 job_log 줄(시간 순서)을 읽는다. 없으면 NotFound."""
    runs = _rows(conn, RUN_SQL, (key, key), RUN_COLUMNS)
    if not runs:
        raise NotFound(key)
    return runs[0], _rows(conn, LOG_SQL, (runs[0]["job_id"],), LOG_COLUMNS)


def _time(value: Any, with_date: bool = False) -> str:
    if not isinstance(value, datetime):
        return str(value)
    return value.strftime("%Y-%m-%d %H:%M:%S") if with_date else value.strftime("%H:%M:%S.") + f"{value.microsecond // 1000:03d}"


def _article(item: Mapping[str, Any]) -> str:
    reason = f" ← {item['reason']}" if item.get("reason") else ""
    return f"{item.get('articleId')} '{item.get('title')}' ({item.get('publishedAt')}){reason}"


def _indent(text: str, prefix: str) -> list[str]:
    return [f"{prefix}{line}" for line in str(text).splitlines() or [""]]


def format_entry(entry: Mapping[str, Any], full: bool = False) -> list[str]:
    """job_log 줄 하나를 읽기 쉬운 몇 줄로 바꾼다."""
    data = entry.get("data") or {}
    head = f"{_time(entry['occurred_at'])}  {entry['entry_type']}  [{entry['stage']}] {entry['reason']}"
    where = []
    if entry.get("sentence_no") is not None:
        where.append(f"문장 {entry['sentence_no']}")
    if entry.get("round") is not None:
        where.append(f"회차 {entry['round']}")
    if entry.get("attempts"):
        where.append(f"시도 {entry['attempts']}")
    if entry.get("error_type"):
        where.append(f"오류 {entry['error_type']}")
    lines = [head + (f"  ({', '.join(where)})" if where else "")]
    pad = "      "
    if data.get("queries"):
        lines.append(f"{pad}검색어: {', '.join(data['queries'])}")
    if data.get("researchJobId"):
        lines.append(f"{pad}재검색 작업: {data['researchJobId']}")
    for label, key in (("받은 기사", "articles"), ("쓸 기사", "usable"), ("쓴 기사", "used"), ("버린 기사", "dropped")):
        if data.get(key):
            lines.append(f"{pad}{label}:")
            lines.extend(f"{pad}  - {_article(item)}" for item in data[key])
    if isinstance(data.get("collectorCompletion"), Mapping):
        done = data["collectorCompletion"]
        lines.append(f"{pad}수집기 완료 알림: 발행 {done.get('publishedCount')}건, "
                     f"못 모은 종목 {len(done.get('incompleteSymbols') or [])}개")
    llm = data.get("llm") or {}
    reject = llm.get("rejectReason") or data.get("rejection")
    if reject:
        lines.append(f"{pad}거부 이유: {reject}")
    if entry.get("before") is not None or entry.get("after") is not None:
        lines.append(f"{pad}before(고치기 전):")
        lines.extend(_indent(entry.get("before") or "", pad + "  "))
        lines.append(f"{pad}after(고친 뒤):")
        lines.extend(_indent(entry.get("after") or "", pad + "  "))
    if full:
        lines.append(f"{pad}data: {json.dumps(data, ensure_ascii=False, default=str)}")
    return lines


def format_history(run: Mapping[str, Any], entries: list[Mapping[str, Any]], full: bool = False) -> str:
    """작업 한 줄(job_run)과 그 아래 줄(job_log)을 시간 순서로 읽기 쉽게 만든다."""
    started, finished = run.get("started_at"), run.get("finished_at")
    took = f"{(finished - started).total_seconds():.0f}초" if isinstance(started, datetime) and \
        isinstance(finished, datetime) else "아직 끝나지 않음"
    lines = [
        f"작업 {run['job_id']}",
        f"  브리핑 ID(메일에 보이는 값): {run.get('result_event_id') or '(발행하지 않음)'}",
        f"  종류 {run['job_kind']}  상태 {run['status']}  작성 {run.get('generated_by') or '-'}  "
        f"대상 {json.dumps(run.get('target'), ensure_ascii=False)}",
        f"  시작 {_time(started, True)}  끝 {_time(finished, True) if finished else '-'}  ({took})",
        f"  근거 약함 {run.get('weak_count')}문장  재검색 요청 {run.get('research_count')}건"
        + (f"  실패 사유 {run['failure_reason']}" if run.get("failure_reason") else ""),
    ]
    if run.get("final_text"):
        lines += ["", "최종 본문:", *_indent(run["final_text"], "  ")]
    lines += ["", f"작업 기록 {len(entries)}줄 (시간 순서):"]
    if not entries:
        lines.append("  (줄이 없습니다)")
    for entry in sorted(entries, key=lambda e: (e["occurred_at"], e["id"])):
        lines.extend(f"  {line}" for line in format_entry(entry, full))
    return "\n".join(lines)


def main(argv: list[str] | None = None, env: Mapping[str, str] | None = None, out: TextIO | None = None,
         connect_fn=connect) -> int:
    parser = argparse.ArgumentParser(description="브리핑 작업 하나의 기록을 시간 순서로 보여 줍니다.")
    parser.add_argument("briefing_id", help="메일에 적힌 브리핑 ID 또는 작업 id(job_id)")
    parser.add_argument("--full", action="store_true", help="줄마다 관련 데이터(LLM 입력·응답 포함)를 JSON으로 덧붙임")
    args = parser.parse_args(argv)
    out = out or sys.stdout
    conn = connect_fn(os.environ if env is None else env)
    try:
        run, entries = load_history(conn, args.briefing_id)
    except NotFound:
        print("그 ID의 작업을 찾지 못했습니다(작업 id와 브리핑 ID를 모두 찾아봤습니다).", file=sys.stderr)
        return 1
    finally:
        conn.close()
    print(format_history(run, entries, args.full), file=out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
