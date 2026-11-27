"""급변동 리포트 단계(ReportStage): 작성기를 불러 리포트를 만들고, 작업 기록을 남기고, briefing.generated 이벤트를 발행한다.

리포트 작성(규칙 기반, LLM)은 report.py, 근거 목록과 LLM 입력은 move_evidence.py에 있다.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.compose import KST, shows_detection
from briefing.events import BRIEFING_GENERATED_TOPIC, OutboundEvent, format_timestamp
from briefing.jobs import ReportInput
from briefing.job_log import (
    REASON_LLM_ACCEPTED,
    REASON_LLM_CALL_FAILED,
    REASON_LLM_REJECTED,
    REASON_NO_EVIDENCE,
    REASON_PUBLISHED,
    STAGE_LLM,
    STAGE_PUBLISH,
    DiscardJobLog,
    EntryType,
    GeneratedBy,
    JobLogEntry,
    JobLogPort,
    JobResult,
    RunStatus,
    record_result,
)
from briefing.move_figures import BASE_EARLIER_MOVE, BASE_PREV_CLOSE, high_today, today_first
from briefing.move_mail import build_move_mail
from briefing.llm import AcceptedAttempt, usage_record
from briefing.ports import FailureReason, RequestPort
from briefing.move_evidence import EvidenceItem, display_name, unchecked_items
from briefing.report import LlmAttempt, LlmOutput, Report, ReportError, ReportWriter, llm_report

# report.py에서 나눈 코드라 운영 로그와 로그를 읽는 확인 도구가 보는 로거 이름을 그대로 둔다.
log = logging.getLogger("briefing.report")


# ---- 발행 ----

def briefing_event(report_input: ReportInput, report: Report, now: datetime, html: str | None = None,
                   images: list[dict[str, str]] | None = None, *, settings: CoreSettings) -> OutboundEvent:
    """html·images는 선택 칸이라 있을 때만 싣는다(데일리 판과 같은 모양). 글자판(text)은 언제나 함께 싣는다."""
    i = report_input
    as_of = format_timestamp(i.as_of)
    return OutboundEvent(
        topic=BRIEFING_GENERATED_TOPIC,
        key=i.symbol,
        value={
            # 결정적 ID: 같은 변동을 다시 처리해도 같은 브리핑 ID가 나와 소비자가 중복을 걸러낼 수 있다.
            "eventId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"briefing:{i.job_id}")),
            "eventType": "briefing.generated",
            "schemaVersion": 1,
            "occurredAt": as_of,
            "producedAt": format_timestamp(now),
            "correlationId": i.correlation_id,
            "payload": {
                "symbol": i.symbol,
                "changePercent": i.change_percent,
                "asOf": as_of,
                "text": report.text,
                "generatedBy": report.generated_by,
                "evidence": [_evidence_entry(e) for e in report.evidence],
                **_name_entry(i),
                **_today_first_entry(i, settings),
                **({"html": html} if html else {}),
                **({"images": images} if html and images else {}),
            },
        },
    )


def _today_first_entry(report_input: ReportInput, settings: CoreSettings) -> dict[str, float | int | str]:
    """메일 제목도 머리말처럼 오늘 등락을 먼저 쓰도록 같은 값을 넘긴다. 상은 baseKind로 기준가 종류(전일 종가·앞선 상)도 알린다. 머리말이 이전 모양이면 칸을 빼고,
    분을 쓰지 않는 머리말('짧은 시간')이면 moveMinutes만 뺀다."""
    high = high_today(report_input)
    if high is not None:
        return {"todayChangePercent": high[0], "baseKind": BASE_PREV_CLOSE if high[1] else BASE_EARLIER_MOVE}
    first = today_first(report_input, settings)
    if first is None:
        return {}
    i = report_input
    return ({"todayChangePercent": first[0]} | ({} if first[1] is None else {"moveMinutes": first[1]})
            | ({"detectedAt": format_timestamp(i.detected_at)} if shows_detection(i.as_of, i.detected_at) else {}))


def _name_entry(report_input: ReportInput) -> dict[str, str]:
    """이름을 알 때만 name 칸을 담는다. 모르거나 비었으면 칸을 뺀다."""
    name = display_name(report_input)
    return {"name": name} if name else {}


@dataclass
class ReportStage:
    """BriefingCore의 ReadyPort 구현. 리포트를 만들면 결과를 기록한 뒤 발행하고, 끝내 못 만들면 llm_error로 기록한다."""

    writer: ReportWriter
    publisher: RequestPort
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))
    settings: CoreSettings = DEFAULT_SETTINGS

    def on_ready(self, report_input: ReportInput) -> None:
        report = self.write(report_input)
        if report is not None:
            self.publish(report_input, report)

    def write(self, report_input: ReportInput, fail_job: bool = True) -> Report | None:
        """리포트를 만든다. 끝내 못 만들면 llm_error로 기록하고 None.

        fail_job이 False이면 작업을 실패로 닫지 않고 시도 기록만 남긴다(이미 낼 수 있는 보고서가 있을 때).
        """
        try:
            report = self.writer.write(report_input)
        except ReportError as error:
            log.warning("job %s failed: %s", report_input.job_id, FailureReason.LLM_ERROR.value)
            now = self.clock()
            self._record_attempts(report_input, error.failed_attempts, now)
            self._record(report_input, EntryType.STEP_FAILED, FailureReason.LLM_ERROR.value, "ReportError",
                         error.attempts, now, {"detail": error.detail,
                                               "failedAttempts": [_attempt_data(a) for a in error.failed_attempts]})
            if fail_job:
                record_result(self.job_log, JobResult(
                    report_input.job_id, RunStatus.FAILED, now, failure_reason=FailureReason.LLM_ERROR.value))
            return None
        now = self.clock()
        self._record_attempts(report_input, report.failed_attempts, now)
        if report.accepted is not None:
            self._record_accepted(report_input, report.accepted, now)
        return report

    def rewrite_sentence(self, report_input: ReportInput, report: Report, sentence_no: int, round: int,
                         new_refs: tuple[str, ...]) -> Report | None:
        """근거가 약한 원인 하나만 다시 쓰고 본문을 다시 만든다. 끝내 못 쓰면 None(원래 보고서를 그대로 둔다).

        report_input에는 이 원인을 위해 새로 찾은 기사까지 이미 들어 있다.
        """
        rewrite = getattr(self.writer, "rewrite_cause", None)
        if rewrite is None or report.output is None:
            return None
        current = report.output.causes[sentence_no - 1]
        result = rewrite(report_input, current, sentence_no, new_refs)
        now = self.clock()
        self._record_attempts(report_input, result.failed_attempts, now, sentence_no, round)
        if result.cause is None:
            return None
        self._record_accepted(report_input, result.accepted, now, sentence_no, round)
        causes = list(report.output.causes)
        causes[sentence_no - 1] = result.cause
        return llm_report(report_input, LlmOutput(tuple(causes), False), self.settings)

    def publish(self, report_input: ReportInput, report: Report, research_count: int = 0) -> None:
        """최종 결과를 job_run에 적고, 이 작업의 줄이 모두 저장됐을 때만 발행한다."""
        now = self.clock()
        if not report.evidence:
            self._skip_without_evidence(report_input, report, research_count, now)
            return
        i = report_input
        event, mail_notes = self._event(i, report, now)
        # 받지 못한 자료와 차트가 빠진 사유는 메일 본문에 싣지 않고 작업 기록에만 남긴다.
        missing = [*unchecked_items(i), *mail_notes]
        if missing:
            self._record(i, EntryType.MISSING_ITEMS, REASON_PUBLISHED, None, 0, now,
                         {"evidence": event.value["payload"]["evidence"], "weakItems": list(report.weak_shortfalls),
                          "missingItems": missing},
                         stage=STAGE_PUBLISH)
        result = JobResult(
            i.job_id, RunStatus.SUCCEEDED, now, generated_by=GeneratedBy(report.generated_by),
            final_text=report.text, weak_count=len(report.weak_shortfalls), research_count=research_count,
            result_event_id=event.value["eventId"])
        if record_result(self.job_log, result):
            self.publisher.send(event)

    def _event(self, i: ReportInput, report: Report, now: datetime) -> tuple[OutboundEvent, tuple[str, ...]]:
        """글자판과 같은 내용의 HTML·차트를 담은 이벤트. 내용(view)이 없는 보고서는 글자판만 싣는다."""
        s = self.settings
        if report.view is None:
            return briefing_event(i, report, now, settings=s), ()
        mail = build_move_mail(i, report.view, s,
                               lambda html, images: briefing_event(i, report, now, html, images, settings=s))
        return briefing_event(i, report, now, mail.html, list(mail.images), settings=s), mail.notes

    def _skip_without_evidence(self, i: ReportInput, report: Report, research_count: int, now: datetime) -> None:
        """본문에 인용한 근거가 하나도 없으면 메일을 보내지 않는다. 결과와 이유는 작업 기록에 남긴다."""
        log.info("job %s has no cited evidence; result event not published", i.job_id)
        self._record(i, EntryType.MISSING_ITEMS, REASON_NO_EVIDENCE, None, 0, now, {
            "receivedCounts": {"articles": len(i.articles), "researchArticles": len(i.research_articles),
                               "filings": len(i.filings), "closeRates": len(i.close_rates),
                               "priceHistory": len(i.price_points)},
            "finalText": report.text}, stage=STAGE_PUBLISH)
        record_result(self.job_log, JobResult(
            i.job_id, RunStatus.SUCCEEDED, now, failure_reason=REASON_NO_EVIDENCE,
            generated_by=GeneratedBy(report.generated_by), final_text=report.text,
            weak_count=len(report.weak_shortfalls), research_count=research_count))

    def _record_attempts(self, report_input: ReportInput, attempts: tuple[LlmAttempt, ...], now: datetime,
                         sentence_no: int | None = None, round: int | None = None) -> None:
        for a in attempts:
            reason = REASON_LLM_CALL_FAILED if a.response is None else REASON_LLM_REJECTED
            self._record(report_input, EntryType.LLM_REJECTED, reason, a.error_type, a.attempt, now,
                         _attempt_data(a), sentence_no=sentence_no, round=round)

    def _record_accepted(self, report_input: ReportInput, a: AcceptedAttempt, now: datetime,
                         sentence_no: int | None = None, round: int | None = None) -> None:
        self._record(report_input, EntryType.REPORT_WRITTEN, REASON_LLM_ACCEPTED, None, a.attempt, now,
                     {"attempt": a.attempt, "llmInput": a.llm_input, "llmResponse": a.response, "rejection": None,
                      "usage": usage_record(a.usage), "articleGroups": a.article_groups},
                     sentence_no=sentence_no, round=round)

    def _record(self, report_input: ReportInput, entry_type: EntryType, reason: str, error_type: str | None,
                attempts: int, now: datetime, extra: dict[str, Any], stage: str = STAGE_LLM,
                sentence_no: int | None = None, round: int | None = None) -> None:
        data: dict[str, Any] = {"missingItems": unchecked_items(report_input)}
        data.update(extra)
        self.job_log.record(JobLogEntry(
            job_id=report_input.job_id, entry_type=entry_type, stage=stage, reason=reason, occurred_at=now,
            sentence_no=sentence_no, round=round, error_type=error_type, attempts=attempts, data=data))


def _attempt_data(a: LlmAttempt) -> dict[str, Any]:
    # 호출 자체가 실패해 응답이 없으면 그 사실을 값으로 남긴다(빈 값으로 두지 않는다).
    return {"attempt": a.attempt, "llmInput": a.llm_input,
            "llmResponse": a.response if a.response is not None else "(응답 없음: 호출 실패)",
            "rejection": a.rejection, "usage": usage_record(a.usage)}


def _evidence_entry(item: EvidenceItem) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "title": item.title,
        "source": item.source,
        "publishedAt": format_timestamp(item.published_at),
        "kind": item.kind,
        "ref": item.ref,
    }
    if item.article_id is not None:
        entry["articleId"] = item.article_id
    if item.url is not None:
        entry["url"] = item.url
    return entry
