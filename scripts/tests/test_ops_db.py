"""확인기 장부(ops 스키마)와 확인기 계정의 마이그레이션·권한을 확인한다.

앞쪽은 파일만 읽는 검사다(항상 돈다): 계정 스크립트가 personal을 열지 않고 읽기 권한만 주는지, 장부 표의 유니크·검사 조건,
마이그레이션 이미지·로컬 Compose·배포 설정에 확인기 계정이 이어져 있는지.
뒤쪽은 실제 Postgres 검사다. OPS_IT_DB_*(확인기 계정)와 MARKET_IT_DB_*·BRIEFING_IT_DB_*(새 표를 만들어 보는 주인 계정)가 모두 있을 때만
실행하고 없으면 건너뛴다. 대상은 scripts/test-all.sh가 market → personal → briefing → ops 순서로 준비한 테스트 전용 DB다.
이 테스트가 만든 줄(event_id가 ops-it-로 시작)과 표(zz_ops_it_probe)만 만들고 끝나면 지운다.
"""
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INIT_SQL = ROOT / "infra/db/ops/init.sql"
V1_SQL = ROOT / "infra/db/ops/migration/V1__create_check_tables.sql"


def read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def sql_lines(path):
    """주석과 빈 줄을 뺀 SQL 줄."""
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("--")]


# ---------------------------------------------------------------- 파일 검사

def test_init_never_opens_personal_or_writes():
    code = "\n".join(sql_lines(INIT_SQL))
    assert "personal" not in code.lower()
    grants = re.findall(r"GRANT\s+(.+?)\s+ON\b", code, flags=re.IGNORECASE)
    assert grants, "권한 줄이 없음"
    for privilege in grants:
        assert privilege.upper() in ("USAGE", "SELECT"), f"읽기 밖의 권한: {privilege}"


def test_init_reads_market_and_briefing_including_future_tables():
    code = "\n".join(sql_lines(INIT_SQL))
    assert re.search(r"GRANT USAGE ON SCHEMA market, briefing TO", code)
    assert re.search(r"GRANT SELECT ON ALL TABLES IN SCHEMA market, briefing TO", code)
    # 기본 권한은 표를 만드는 계정별이라 market·briefing 계정 각각에 있어야 한다.
    assert re.search(r'ALTER DEFAULT PRIVILEGES FOR ROLE :"market_user" IN SCHEMA market GRANT SELECT ON TABLES TO', code)
    assert re.search(r'ALTER DEFAULT PRIVILEGES FOR ROLE :"briefing_user" IN SCHEMA briefing GRANT SELECT ON TABLES TO', code)


def test_init_is_idempotent_and_keeps_existing_password():
    code = "\n".join(sql_lines(INIT_SQL))
    assert "WHERE NOT EXISTS (SELECT 1 FROM pg_roles" in code
    assert "CREATE SCHEMA IF NOT EXISTS ops AUTHORIZATION" in code
    assert "ALTER ROLE" not in code.upper()


def test_ledger_tables_keep_one_run_per_event_and_one_result_per_metric():
    code = "\n".join(sql_lines(V1_SQL))
    assert re.search(r"event_id\s+VARCHAR\(64\)\s+NOT NULL UNIQUE", code)
    assert "UNIQUE (run_id, issue, attempt, metric)" in code
    for word in ("criteria_hash", "threshold", "verdict", "market_status", "deploy_version", "outcome"):
        assert word in code


def test_migrate_script_runs_ops_after_market_and_briefing():
    script = read("infra/deploy/migrate/migrate.sh")
    order = [script.index(token) for token in
             ("flyway_migrate market ", "flyway_migrate briefing ", "/sql/ops-init.sql", "flyway_migrate ops ")]
    assert order == sorted(order)
    assert '"$OPS_CHECKER_DB_USER" "$OPS_CHECKER_DB_PASSWORD" /sql/ops' in script


def test_migrate_image_carries_ops_files():
    dockerfile = read("infra/deploy/migrate/Dockerfile")
    assert "COPY infra/db/ops/init.sql /sql/ops-init.sql" in dockerfile
    assert "COPY infra/db/ops/migration /sql/ops" in dockerfile


def test_deploy_compose_and_secret_list_pass_the_checker_account():
    services = read("infra/deploy/compose/services.yml")
    assert "OPS_CHECKER_DB_PASSWORD: ${OPS_CHECKER_DB_PASSWORD:?" in services
    assert "OPS_CHECKER_DB_USER: ${OPS_CHECKER_DB_USER:-ops_checker}" in services
    assert "OPS_CHECKER_DB_PASSWORD" in read("infra/deploy/server/apply-deploy.sh")


def test_local_compose_runs_ops_steps_after_market_and_briefing():
    compose = read("infra/compose/docker-compose.yml")
    init = compose[compose.index("  ops-init:"):compose.index("  ops-migrate:")]
    assert "market-migrate:" in init and "briefing-migrate:" in init
    assert "../db/ops/init.sql:/init/init.sql:ro" in init
    migrate = compose[compose.index("  ops-migrate:"):compose.index("  kafka-ui:")]
    assert "ops-init:" in migrate
    assert "FLYWAY_SCHEMAS: ops" in migrate and "../db/ops/migration:/flyway/sql:ro" in migrate


def test_env_example_has_checker_account():
    env = read(".env.example")
    assert re.search(r"^OPS_CHECKER_DB_USER=ops_checker$", env, flags=re.MULTILINE)
    assert re.search(r"^OPS_CHECKER_DB_PASSWORD=change-me$", env, flags=re.MULTILINE)


def test_test_all_prepares_ops_after_briefing():
    script = read("scripts/test-all.sh")
    assert script.index("migrate_briefing; then") < script.index("init_ops; then") < script.index("migrate_ops; then")
    assert "OPS_CHECKER_DB_USER OPS_CHECKER_DB_PASSWORD" in script


# ---------------------------------------------------------------- 실제 DB 검사

_VARS = ("OPS_IT_DB_URL", "OPS_IT_DB_USER", "OPS_IT_DB_PASSWORD", "MARKET_IT_DB_USER", "MARKET_IT_DB_PASSWORD",
         "BRIEFING_IT_DB_USER", "BRIEFING_IT_DB_PASSWORD")

needs_db = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="실제 DB 테스트: OPS_IT_DB_*·MARKET_IT_DB_*·BRIEFING_IT_DB_* 가 없음",
)

PROBE = "zz_ops_it_probe"
EVENT = "ops-it-%s"
HASH = "a" * 64


def connect(user_key, password_key):
    import psycopg
    from psycopg.conninfo import make_conninfo

    return psycopg.connect(make_conninfo(os.environ["OPS_IT_DB_URL"], user=os.environ[user_key],
                                         password=os.environ[password_key]), autocommit=True)


@pytest.fixture
def ops():
    connection = connect("OPS_IT_DB_USER", "OPS_IT_DB_PASSWORD")
    yield connection
    connection.execute("DELETE FROM ops.check_run WHERE event_id LIKE 'ops-it-%'")
    connection.close()


def new_run(ops, key, outcome="ok", error=None, finished=True):
    return ops.execute(
        "INSERT INTO ops.check_run (day, edition, event_id, market_status, deploy_version, started_at, finished_at, "
        "outcome, error) VALUES ('2026-11-17', 'post-close', %s, 'collected', 'v-test', now(), "
        "CASE WHEN %s THEN now() END, %s, %s) RETURNING id",
        (EVENT % key, finished, outcome, error)).fetchone()[0]


def new_result(ops, run_id, metric="m1", verdict="pass", value=0.5, reason=None, issue=52, attempt="1", digest=HASH):
    ops.execute(
        "INSERT INTO ops.check_result (run_id, issue, attempt, criteria_hash, metric, value, threshold, verdict, reason) "
        "VALUES (%s, %s, %s, %s, %s, %s, '< 0.9', %s, %s)",
        (run_id, issue, attempt, digest, metric, value, verdict, reason))


@needs_db
def test_ops_account_writes_the_ledger_and_reads_it_back(ops):
    run_id = new_run(ops, "a")
    new_result(ops, run_id)
    new_result(ops, run_id, metric="m2", verdict="hold", value=None, reason="zero-denominator")
    rows = ops.execute("SELECT metric, verdict, value FROM ops.check_result WHERE run_id = %s ORDER BY metric",
                       (run_id,)).fetchall()
    assert rows == [("m1", "pass", 0.5), ("m2", "hold", None)]


@needs_db
def test_same_event_runs_only_once(ops):
    import psycopg

    new_run(ops, "dup")
    with pytest.raises(psycopg.errors.UniqueViolation):
        new_run(ops, "dup")
    assert ops.execute("SELECT count(*) FROM ops.check_run WHERE event_id = %s", (EVENT % "dup",)).fetchone()[0] == 1


@needs_db
def test_same_metric_of_one_attempt_is_stored_once_per_run(ops):
    import psycopg

    run_id = new_run(ops, "metric")
    new_result(ops, run_id)
    with pytest.raises(psycopg.errors.UniqueViolation):
        new_result(ops, run_id)
    new_result(ops, run_id, attempt="2")
    new_result(ops, run_id, issue=53)


@needs_db
@pytest.mark.parametrize("kwargs", [
    {"value": 1.5},                                  # 비율은 0~1
    {"value": None},                                 # pass 는 값이 있어야 함
    {"verdict": "hold", "value": 0.1, "reason": "x"},  # hold 는 값이 비어야 함
    {"verdict": "hold", "value": None},              # hold·error 는 종류가 있어야 함
    {"verdict": "unknown"},
    {"digest": "xyz"},                               # 지문은 SHA-256 소문자 16진수
])
def test_result_rejects_malformed_rows(ops, kwargs):
    import psycopg

    run_id = new_run(ops, "bad")
    with pytest.raises(psycopg.errors.CheckViolation):
        new_result(ops, run_id, **kwargs)


@needs_db
def test_error_run_must_name_the_kind_of_failure(ops):
    import psycopg

    new_run(ops, "err", outcome="error", error="upload-failed")
    with pytest.raises(psycopg.errors.CheckViolation):
        new_run(ops, "err2", outcome="error")


@needs_db
def test_run_finished_at_and_outcome_come_together(ops):
    import psycopg

    running = new_run(ops, "running", outcome=None, finished=False)
    ops.execute("UPDATE ops.check_run SET finished_at = now(), outcome = 'ok' WHERE id = %s", (running,))
    with pytest.raises(psycopg.errors.CheckViolation):
        new_run(ops, "only-outcome", outcome="ok", finished=False)
    with pytest.raises(psycopg.errors.CheckViolation):
        new_run(ops, "only-finished", outcome=None, finished=True)


@needs_db
def test_deleting_a_run_removes_its_results(ops):
    run_id = new_run(ops, "cascade")
    new_result(ops, run_id)
    ops.execute("DELETE FROM ops.check_run WHERE id = %s", (run_id,))
    assert ops.execute("SELECT count(*) FROM ops.check_result WHERE run_id = %s", (run_id,)).fetchone()[0] == 0


@needs_db
def test_ops_account_reads_every_market_and_briefing_table_and_writes_none(ops):
    rows = ops.execute(
        "SELECT n.nspname, c.relname, has_table_privilege(current_user, c.oid, 'SELECT'), "
        "       has_table_privilege(current_user, c.oid, 'INSERT') OR has_table_privilege(current_user, c.oid, 'UPDATE') "
        "       OR has_table_privilege(current_user, c.oid, 'DELETE') "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('market', 'briefing') AND c.relkind IN ('r', 'p', 'v', 'm', 'f')").fetchall()
    assert any(schema == "market" for schema, *_ in rows) and any(schema == "briefing" for schema, *_ in rows)
    assert [(s, t) for s, t, can_read, _ in rows if not can_read] == []
    assert [(s, t) for s, t, _, can_write in rows if can_write] == []


@needs_db
def test_ops_account_has_no_access_to_personal(ops):
    import psycopg

    assert ops.execute("SELECT has_schema_privilege(current_user, 'personal', 'USAGE')").fetchone()[0] is False
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        ops.execute("SELECT * FROM personal.member_mail LIMIT 1")


@needs_db
@pytest.mark.parametrize("schema, owner_keys", [
    ("market", ("MARKET_IT_DB_USER", "MARKET_IT_DB_PASSWORD")),
    ("briefing", ("BRIEFING_IT_DB_USER", "BRIEFING_IT_DB_PASSWORD")),
])
def test_tables_created_later_are_readable_but_not_writable(ops, schema, owner_keys):
    import psycopg

    owner = connect(*owner_keys)
    try:
        owner.execute(f"DROP TABLE IF EXISTS {schema}.{PROBE}")
        owner.execute(f"CREATE TABLE {schema}.{PROBE} (id int)")
        owner.execute(f"INSERT INTO {schema}.{PROBE} VALUES (1)")
        assert ops.execute(f"SELECT id FROM {schema}.{PROBE}").fetchall() == [(1,)]
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            ops.execute(f"INSERT INTO {schema}.{PROBE} VALUES (2)")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            ops.execute(f"DROP TABLE {schema}.{PROBE}")
    finally:
        owner.execute(f"DROP TABLE IF EXISTS {schema}.{PROBE}")
        owner.close()


@needs_db
def test_read_only_transaction_rejects_changes_even_in_ops(ops):
    import psycopg

    with ops.transaction():
        ops.execute("SET TRANSACTION READ ONLY")
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            ops.execute("INSERT INTO ops.check_run (day, edition, event_id, market_status, deploy_version, started_at) "
                        "VALUES ('2026-11-17', 'post-close', 'ops-it-ro', 'collected', 'v', now())")
