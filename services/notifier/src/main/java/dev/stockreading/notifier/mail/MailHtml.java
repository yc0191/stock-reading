package dev.stockreading.notifier.mail;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.stockreading.notifier.event.ImageData;
import dev.stockreading.notifier.mail.EmailMessage.InlineImage;

/** 이벤트가 싣고 온 HTML 본문과 인라인 그림을 메일로 옮기는 공통 부분(데일리·급변동 메일). */
final class MailHtml {

    private static final Logger log = LoggerFactory.getLogger(MailHtml.class);
    private static final Set<String> ALLOWED_TYPES = Set.of("image/png", "image/jpeg", "image/gif");
    // 공백·꺾쇠·줄바꿈이 있으면 메일 헤더(Content-ID)가 깨지거나 주입될 수 있다.
    private static final Pattern VALID_CONTENT_ID = Pattern.compile("[^\\s<>]+");
    private static final Pattern BODY_END = Pattern.compile("</body\\s*>", Pattern.CASE_INSENSITIVE);

    private MailHtml() {
    }

    static boolean hasHtml(String html) {
        return html != null && !html.isBlank();
    }

    /** 그림 바이트 목록. 잘못된 그림은 그 그림만 빼고, 메일은 나머지로 보낸다. */
    static List<InlineImage> decodeImages(List<? extends ImageData> images, String eventId) {
        List<InlineImage> decoded = new ArrayList<>();
        if (images == null) {
            return decoded;
        }
        int badFormat = 0;
        int badId = 0;
        int badData = 0;
        for (ImageData image : images) {
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
            log.warn("briefing images dropped eventId={} badFormat={} badContentId={} badData={}",
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

    /** 글자판의 꼬리와 같은 내용(줄 목록)을 HTML 끝에 작은 글씨로 붙인다. 값은 HTML로 이스케이프한다. */
    static String withFooter(String html, List<String> footerLines) {
        List<String> escaped = new ArrayList<>();
        for (String line : footerLines) {
            escaped.add(escape(line));
        }
        String footer = "<div style=\"margin-top:24px;padding-top:8px;border-top:1px solid #ddd;"
                + "font-size:11px;color:#888;line-height:1.5\">"
                + String.join("<br>", escaped)
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
}
