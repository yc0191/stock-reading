package dev.stockreading.notifier.mail;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

import org.springframework.stereotype.Component;

import dev.stockreading.notifier.NotifierProperties;
import dev.stockreading.notifier.event.BriefingEnvelope;

/** 브리핑으로 이메일을 만든다. 공개 정보만 쓴다: 보유 종목·수량은 넣지 않는다. */
@Component
public class EmailComposer {

    /** 급변동 메일의 보낸 사람 이름. 데일리 메일과 받은편지함에서 구분되게 한다. */
    static final String FROM_NAME = "변동 감지";

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter HH_MM = DateTimeFormatter.ofPattern("HH:mm");

    private final String subjectPrefix;

    public EmailComposer(NotifierProperties properties) {
        this.subjectPrefix = properties.subjectPrefix() == null ? "" : properties.subjectPrefix().trim();
    }

    public EmailMessage compose(BriefingEnvelope briefing) {
        BriefingEnvelope.Payload payload = briefing.payload();
        // 이름 칸은 선택이라 없거나 비면 코드만 쓴다.
        String label = payload.name() == null || payload.name().isBlank()
                ? payload.symbol() : payload.name().trim() + "(" + payload.symbol() + ")";
        String core = todayFirst(payload, label);
        if (core == null) {
            core = "[%s] %s %+.2f%% 브리핑".formatted(label, direction(payload.changePercent()), payload.changePercent());
        }
        String subject = subjectPrefix.isEmpty() ? core : subjectPrefix + " " + core;

        List<String> footer = List.of(
                "브리핑 ID: " + briefing.eventId(),
                "추적 ID: " + briefing.correlationId(),
                "생성 방식: " + payload.generatedBy());
        String body = payload.text() + "\n\n----------------------------------------\n" + String.join("\n", footer);
        // html이 있으면 글자판과 함께 묶음 메일로, 없으면(이전 형식) 글자판만 보낸다.
        if (!MailHtml.hasHtml(payload.html())) {
            return new EmailMessage(subject, body, null, List.of(), FROM_NAME);
        }
        return new EmailMessage(subject, body, MailHtml.withFooter(payload.html(), footer),
                MailHtml.decodeImages(payload.images(), briefing.eventId()), FROM_NAME);
    }

    /**
     * 레벨 상(baseKind 있음)도 같은 방식으로 오늘 등락을 앞에 쓴다.
     * 짧은 시간 변동은 몇 분 사이의 변동률보다 그날 흐름이 먼저 보이게, 본문 첫 줄처럼 오늘 등락을 앞에 쓴다.
     * 오늘 등락률이 없는 이전 브리핑이면 null을 돌려 이전 제목을 쓰게 한다.
     * 분이 없으면 브리핑이 구간을 정확히 알 수 없다고 본 것(다시 보낸 변동)이라 '짧은 시간'으로 쓴다.
     */
    private static String todayFirst(BriefingEnvelope.Payload payload, String label) {
        if (payload.todayChangePercent() == null) {
            return null;
        }
        String time;
        try {
            time = OffsetDateTime.parse(payload.asOf()).atZoneSameInstant(KST).format(HH_MM);
        } catch (DateTimeParseException e) {
            return null;
        }
        BigDecimal today = payload.todayChangePercent();
        BigDecimal change = payload.changePercent();
        if (payload.baseKind() != null) {
            // 레벨 상: 첫 상은 기준가가 전일 종가라 오늘 등락만, 그다음 상은 기준가 대비 변동률을 덧붙인다.
            if ("prevClose".equals(payload.baseKind())) {
                return "[%s] %s 기준 오늘 %+.2f%% %s 브리핑".formatted(label, time, today, direction(today));
            }
            return "[%s] %s 기준 오늘 %+.2f%% · 기준가 대비 %+.2f%% %s 브리핑".formatted(
                    label, time, today, change, windowDirection(change, today));
        }
        String span = payload.moveMinutes() == null ? "짧은 시간" : payload.moveMinutes() + "분 새";
        return "[%s] %s 기준 오늘 %+.2f%% · %s %+.2f%% %s 브리핑".formatted(
                label, time, today, span, change, windowDirection(change, today));
    }

    /** 오늘 등락과 방향이 반대면 반등·반락, 같거나 오늘 등락이 0.00이면 상승·하락. 브리핑 첫 줄과 같은 규칙이다. */
    private static String windowDirection(BigDecimal change, BigDecimal today) {
        int todaySign = today.setScale(2, RoundingMode.HALF_UP).signum();
        boolean up = change.compareTo(BigDecimal.ZERO) > 0;
        if (todaySign != 0 && (todaySign > 0) != up) {
            return up ? "반등" : "반락";
        }
        return direction(change);
    }

    private static String direction(BigDecimal change) {
        return change.compareTo(BigDecimal.ZERO) > 0 ? "상승" : "하락";
    }
}
