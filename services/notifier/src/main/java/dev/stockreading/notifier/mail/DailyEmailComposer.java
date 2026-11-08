package dev.stockreading.notifier.mail;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.notifier.NotifierProperties;
import dev.stockreading.notifier.event.DailyBriefingEnvelope;
import dev.stockreading.notifier.mail.EmailMessage.InlineImage;

/** 데일리 브리핑 한 판으로 이메일 한 통을 만든다. 이벤트에 개인 정보가 없으므로 본문은 받은 글을 그대로 쓴다. */
@Component
public class DailyEmailComposer {

    private static final Logger log = LoggerFactory.getLogger(DailyEmailComposer.class);

    private static final Set<String> ALLOWED_TYPES = Set.of("image/png", "image/jpeg", "image/gif");
    // 공백·꺾쇠·줄바꿈이 있으면 메일 헤더(Content-ID)가 깨지거나 주입될 수 있다.
    private static final Pattern VALID_CONTENT_ID = Pattern.compile("[^\\s<>]+");
    private static final Pattern BODY_END = Pattern.compile("</body\\s*>", Pattern.CASE_INSENSITIVE);

    private final String subjectPrefix;

    public DailyEmailComposer(NotifierProperties properties) {
        this.subjectPrefix = properties.subjectPrefix() == null ? "" : properties.subjectPrefix().trim();
    }

    /** @throws java.time.format.DateTimeParseException rangeEnd가 날짜·시각 형식이 아닐 때 */
    public EmailMessage compose(DailyBriefingEnvelope daily) {
        DailyBriefingEnvelope.Payload payload = daily.payload();
        // 판의 날짜는 이번 발송 시각(rangeEnd)의 날짜다. 시각에 붙은 오프셋(한국 시간) 그대로 읽는다.
        LocalDate date = OffsetDateTime.parse(payload.rangeEnd()).toLocalDate();
        String core = "%s 데일리 브리핑 %s".formatted(editionLabel(payload.edition()), date);
        String subject = subjectPrefix.isEmpty() ? core : subjectPrefix + " " + core;

        String body = payload.text()
                + "\n\n----------------------------------------"
                + "\n자료 없음 항목: " + missingItems(payload.missingItems())
                + "\n다룬 범위: " + payload.rangeStart() + " ~ " + payload.rangeEnd()
                + "\n브리핑 ID: " + daily.eventId()
                + "\n추적 ID: " + daily.correlationId()
                + "\n생성 방식: " + payload.generatedBy();
        if (payload.html() == null || payload.html().isBlank()) {
            return new EmailMessage(subject, body);
        }
        return new EmailMessage(subject, body, withFooter(payload.html(), daily), decodeImages(payload.images(), daily.eventId()));
    }

    /** 그림 바이트 목록. 잘못된 그림은 그 그림만 빼고, 메일은 나머지로 보낸다. */
    private static List<InlineImage> decodeImages(List<DailyBriefingEnvelope.Image> images, String eventId) {
        List<InlineImage> decoded = new ArrayList<>();
        if (images == null) {
            return decoded;
        }
        int badFormat = 0;
        int badId = 0;
        int badData = 0;
        for (DailyBriefingEnvelope.Image image : images) {
            if (image == null || image.contentType() == null || !ALLOWED_TYPES.contains(image.contentType())) {
                badFormat++;
            } else if (image.contentId() == null || !VALID_CONTENT_ID.matcher(image.contentId()).matches()) {
                badId++;
            } else {
                byte[] data = decode(image.data());
                if (data == null || data.length == 0) {
                    badData++;
                } else {
                    decoded.add(new InlineImage(image.contentId(), image.contentType(), data));
                }
            }
        }
        if (badFormat + badId + badData > 0) {
            // 재시도해도 고쳐지지 않으므로 그림만 빼고 보낸다. 값·base64는 남기지 않는다.
            log.warn("daily briefing images dropped eventId={} badFormat={} badContentId={} badData={}",
                    eventId, badFormat, badId, badData);
        }
        return decoded;
    }

    private static byte[] decode(String data) {
        if (data == null) {
            return null;
        }
        try {
            return Base64.getDecoder().decode(data);
        } catch (IllegalArgumentException error) {
            return null;
        }
    }

    /** 글자판의 꼬리와 같은 내용을 HTML 끝에 작은 글씨로 붙인다. 값은 HTML로 이스케이프한다. */
    private static String withFooter(String html, DailyBriefingEnvelope daily) {
        DailyBriefingEnvelope.Payload payload = daily.payload();
        String footer = "<div style=\"margin-top:24px;padding-top:8px;border-top:1px solid #ddd;"
                + "font-size:11px;color:#888;line-height:1.5\">"
                + "자료 없음 항목: " + escape(missingItems(payload.missingItems()))
                + "<br>다룬 범위: " + escape(payload.rangeStart() + " ~ " + payload.rangeEnd())
                + "<br>브리핑 ID: " + escape(daily.eventId())
                + "<br>추적 ID: " + escape(daily.correlationId())
                + "<br>생성 방식: " + escape(payload.generatedBy())
                + "</div>";
        // 소문자로 바꾸면 길이가 달라지는 글자가 있으므로, 원문에서 대소문자를 무시하고 마지막 </body>를 찾는다.
        Matcher matcher = BODY_END.matcher(html);
        int bodyEnd = -1;
        while (matcher.find()) {
            bodyEnd = matcher.start();
        }
        return bodyEnd < 0 ? html + footer : html.substring(0, bodyEnd) + footer + html.substring(bodyEnd);
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private static String editionLabel(String edition) {
        return switch (edition) {
            case "morning" -> "아침 판";
            case "post-close" -> "마감 후 판";
            // 스키마가 막지만, 새 판 종류가 먼저 들어와도 메일은 나가도록 원래 값을 보여 준다.
            default -> edition;
        };
    }

    private static String missingItems(List<String> items) {
        return items == null || items.isEmpty() ? "없음" : String.join(", ", items);
    }
}
