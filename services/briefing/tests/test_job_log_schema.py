"""Flyway V1~V5 파일(services/briefing/db/migration)이 코드가 기대하는 테이블 모양과 같은지 글자로 확인한다. DB는 부르지 않는다.

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
V3 = (MIGRATIONS / "V3__add_final_check_entry_types.sql").read_text(encoding="utf-8")
V4 = (MIGRATIONS / "V4__create_market_closed_edition.sql").read_text(encoding="utf-8")
V5 = (MIGRATIONS / "V5__create_job_step.sql").read_text(encoding="utf-8")


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
                                                MIGRATIONS / "V2__add_member_briefing_entry_type.sql",
                                                MIGRATIONS / "V3__add_final_check_entry_types.sql",
                                                MIGRATIONS / "V4__create_market_closed_edition.sql",
                                                MIGRATIONS / "V5__create_job_step.sql"]


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
    # 기록 종류의 값 제한은 V3이 다시 걸어(V4는 휴장 기록 표만 만듦) 코드 enum 전체와 같다. V1에는 member-briefing이, V2에는
    # 마지막 검사의 두 값이 아직 없다.
    final_check = {EntryType.FINAL_CHECK.value, EntryType.SENTENCE_FIXED.value}
    assert entry_type_values(V3) == {m.value for m in EntryType}
    assert entry_type_values(V2) == {m.value for m in EntryType} - final_check
    assert checked_values("job_log", "entry_type") == \
        {m.value for m in EntryType} - {EntryType.MEMBER_BRIEFING.value} - final_check


def entry_type_values(sql):
    return set(re.findall(r"'([^']+)'", re.search(r"CHECK \(entry_type IN \((.*?)\)\)", sql, re.S).group(1)))


def test_v3_only_widens_the_entry_type_check_like_v2_and_changes_no_data():
    code = re.sub(r"--[^\n]*", "", V3)
    statements = [s.strip() for s in code.split(";") if s.strip()]
    assert len(statements) == 2
    assert statements[0] == "ALTER TABLE job_log DROP CONSTRAINT job_log_entry_type_check"
    assert statements[1].startswith("ALTER TABLE job_log ADD CONSTRAINT job_log_entry_type_check CHECK (entry_type IN (")
    for word in ("INSERT", "UPDATE", "DELETE"):
        assert word not in code.upper()


def test_the_collection_table_matches_what_the_status_store_writes():
    from briefing.status_store import INSERT_COLLECTION_SQL

    columns = re.search(r"\((.*?)\)\s*VALUES", INSERT_COLLECTION_SQL, re.S).group(1)
    for column in [c.strip() for c in columns.split(",")]:
        assert re.search(rf"^\s+{column}\s", table_sql("collection_completed"), re.M), column


def test_the_market_closed_table_takes_only_the_two_daily_editions_once_a_day():
    # 휴장 기록은 판 작업이 아니므로 job_run이 아닌 따로 된 표에 둔다(서버 상태 절의 판·실패 건수에 세지지 않게).
    match = re.search(r"CREATE TABLE market_closed_edition \((.*?)\n\);", V4, re.S)
    assert match and "IF NOT EXISTS" not in re.sub(r"--[^\n]*", "", V4).upper()
    values = set(re.findall(r"'([^']+)'", re.search(r"CHECK \(edition IN \((.*?)\)\)", match.group(1)).group(1)))
    assert values == {JobKind.MORNING.value, JobKind.POST_CLOSE.value}
    assert "PRIMARY KEY (edition_day, edition)" in match.group(1)
    from briefing.job_log_store import INSERT_MARKET_CLOSED_SQL

    columns = re.search(r"\((.*?)\)\s*VALUES", INSERT_MARKET_CLOSED_SQL, re.S).group(1)
    for column in [c.strip() for c in columns.split(",")]:
        assert re.search(rf"^\s+{column}\s", match.group(1), re.M), column


def test_the_job_step_table_holds_one_call_per_line_under_its_job_run():
    from briefing.job_log_store import INSERT_STEP_SQL
    from briefing.job_step import StepResult

    # 호출 한 줄(조회·LLM의 시도 하나)은 그 작업(job_run) 아래에 남고, 메일 하나의 추적 ID로 찾는다.
    match = re.search(r"CREATE TABLE job_step \((.*?)\n\);", V5, re.S)
    assert match and "IF NOT EXISTS" not in re.sub(r"--[^\n]*", "", V5).upper()
    table = match.group(1)
    assert "REFERENCES job_run (job_id)" in table
    values = set(re.findall(r"'([^']+)'", re.search(r"CHECK \(result IN \((.*?)\)\)", table).group(1)))
    assert values == {m.value for m in StepResult}
    columns = re.search(r"\((.*?)\)\s*VALUES", INSERT_STEP_SQL, re.S).group(1)
    for column in [c.strip() for c in columns.split(",")]:
        assert re.search(rf"^\s+{column}\s", table, re.M), column
    assert "job_step (correlation_id)" in V5
