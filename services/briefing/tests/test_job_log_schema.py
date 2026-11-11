"""Flyway V1 파일(services/briefing/db/migration)이 코드가 기대하는 테이블 모양과 같은지 글자로 확인한다. DB는 부르지 않는다.

값 제한(CHECK)은 코드 enum과 SQL 파일 양쪽에 있으므로 어긋나면 여기서 실패한다.
실제 Postgres에 적용한 모양은 test_job_log_store_db.py가 확인한다.
"""
import re
from pathlib import Path

from briefing.job_log import EntryType, GeneratedBy, JobKind, RunStatus

MIGRATIONS = Path(__file__).resolve().parents[1] / "db" / "migration"
V1 = (MIGRATIONS / "V1__create_briefing_tables.sql").read_text(encoding="utf-8")
V1_CODE = re.sub(r"--[^\n]*", "", V1)  # 주석에는 JSONB 같은 말이 나올 수 있다
V2 = (MIGRATIONS / "V2__add_member_briefing_entry_type.sql").read_text(encoding="utf-8")


def table_sql(name):
    match = re.search(rf"CREATE TABLE {name} \((.*?)\n\);", V1, re.S)
    assert match, f"{name} 생성문이 없다"
    return match.group(1)


def checked_values(table, column):
    match = re.search(rf"{column}\s+VARCHAR\(\d+\)(?: NOT NULL)? CHECK \(\s*{column} IN \((.*?)\)\)", table_sql(table), re.S)
    assert match, f"{table}.{column}에 값 제한이 없다"
    return set(re.findall(r"'([^']+)'", match.group(1)))


def test_the_migration_creates_the_three_tables_without_if_not_exists():
    for name in ("job_run", "job_log", "collection_completed"):
        table_sql(name)
    assert "IF NOT EXISTS" not in V1_CODE.upper()
    assert sorted(MIGRATIONS.glob("V*.sql")) == [MIGRATIONS / "V1__create_briefing_tables.sql",
                                                MIGRATIONS / "V2__add_member_briefing_entry_type.sql"]


def test_run_and_log_columns_follow_the_plan_and_keep_json_as_json():
    run, log = table_sql("job_run"), table_sql("job_log")
    for column in ("job_id", "job_kind", "input", "status", "failure_reason", "generated_by", "target", "started_at",
                   "finished_at", "final_text", "weak_count", "research_count", "result_event_id"):
        assert re.search(rf"^\s+{column}\s", run, re.M), column
    for column in ("id", "job_id", "entry_type", "stage", "sentence_no", "round", "before", "after", "reason",
                   "error_type", "attempts", "data", "occurred_at"):
        assert re.search(rf"^\s+{column}\s", log, re.M), column
    # 시작시킨 이벤트 원문, 대상, 관련 데이터, 받은 원문은 표기를 바꾸는 JSONB가 아니라 JSON이다.
    assert "JSONB" not in V1_CODE.upper()
    assert re.search(r"input\s+JSON NOT NULL", run) and re.search(r"data\s+JSON NOT NULL", log)
    assert re.search(r"raw\s+JSON NOT NULL", table_sql("collection_completed"))
    assert "REFERENCES job_run (job_id)" in log


def test_value_checks_match_the_code_enums():
    assert checked_values("job_run", "job_kind") == {m.value for m in JobKind}
    assert checked_values("job_run", "status") == {m.value for m in RunStatus}
    assert checked_values("job_run", "generated_by") == {m.value for m in GeneratedBy}
    # 기록 종류의 값 제한은 V2가 다시 걸어 코드 enum 전체와 같다(V1에는 member-briefing이 아직 없다).
    v2_values = set(re.findall(r"'([^']+)'", re.search(r"CHECK \(entry_type IN \((.*?)\)\)", V2, re.S).group(1)))
    assert v2_values == {m.value for m in EntryType}
    assert checked_values("job_log", "entry_type") == {m.value for m in EntryType} - {EntryType.MEMBER_BRIEFING.value}


def test_the_collection_table_matches_what_the_status_store_writes():
    from briefing.status_store import INSERT_COLLECTION_SQL

    columns = re.search(r"\((.*?)\)\s*VALUES", INSERT_COLLECTION_SQL, re.S).group(1)
    for column in [c.strip() for c in columns.split(",")]:
        assert re.search(rf"^\s+{column}\s", table_sql("collection_completed"), re.M), column
