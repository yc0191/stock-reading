"""S3 결과 파일. 비율·판정·버전·지문만 담는다: 건수, 종목, 서버 주소, SQL 본문, DB 원본은 넣지 않는다."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Protocol

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from ops_checker.criteria import Criteria
from ops_checker.metrics import MetricResult

EDITION = "post-close"


class UploadError(Exception):
    """올리기 실패. 메시지는 종류 이름뿐이다(비밀값·버킷 이름 없음)."""


class ResultStore(Protocol):
    def put(self, key: str, body: bytes) -> None: ...


@dataclass(frozen=True)
class CheckEntry:
    criteria: Criteria
    metrics: tuple[MetricResult, ...]


def result_key(day: date) -> str:
    return f"results/{day.isoformat()}/{EDITION}.json"


def build_result(day: date, status: str, deploy_version: str, entries: list[CheckEntry]) -> bytes:
    checks = [{
        "issue": e.criteria.issue,
        "attempt": e.criteria.attempt,
        "criteria_hash": e.criteria.criteria_hash,
        "comment": e.criteria.comment,
        "days": e.criteria.days,
        "metrics": [{"key": m.key,
                     "value": None if m.value is None else float(m.value),
                     "pass": m.threshold,
                     "verdict": m.verdict,
                     "reason": m.reason} for m in e.metrics],
    } for e in entries]
    doc = {"day": day.isoformat(), "edition": EDITION, "status": status,
           "deploy_version": deploy_version, "checks": checks}
    return json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8")


class S3ResultStore:
    def __init__(self, bucket: str, client=None) -> None:
        self._bucket = bucket
        self._client = client or boto3.client("s3")

    def put(self, key: str, body: bytes) -> None:
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
        except (BotoCoreError, ClientError) as error:
            raise UploadError("upload-failed") from error
