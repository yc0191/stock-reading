"""환경변수에서 읽는 설정. 비밀번호는 로그에 남기지 않는다."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# 수집기 판 시작 알림. 파티션 수는 수집기(Topics.java)와 같아야 한다.
DAILY_COLLECTION_COMPLETED_TOPIC = "daily.collection.completed"
DAILY_COLLECTION_COMPLETED_PARTITIONS = 1


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str = field(repr=False)
    results_bucket: str
    deploy_version: str
    criteria_dir: Path

    @staticmethod
    def from_env(env: dict[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env

        def need(name: str) -> str:
            value = env.get(name, "")
            if not value:
                raise ValueError(f"{name} 값이 비어 있음")
            return value

        return Settings(
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
            db_host=env.get("DB_HOST", "localhost"),
            db_port=int(env.get("DB_PORT", "5432")),
            db_name=env.get("POSTGRES_DB", "stock"),
            db_user=env.get("OPS_CHECKER_DB_USER", "ops_checker"),
            db_password=need("OPS_CHECKER_DB_PASSWORD"),
            results_bucket=need("OPS_RESULTS_BUCKET"),
            deploy_version=env.get("DEPLOY_VERSION", "dev"),
            criteria_dir=Path(env.get("OPS_CRITERIA_DIR", "/app/ops-checks")),
        )
