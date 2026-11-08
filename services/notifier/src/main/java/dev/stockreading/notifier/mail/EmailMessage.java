package dev.stockreading.notifier.mail;

import java.util.List;

/**
 * 보낼 메일 한 통. {@code html}이 있으면 글자판({@code body})과 HTML을 함께 담은 묶음 메일로,
 * 없으면 글자 메일로 나간다. {@code images}는 HTML이 {@code cid:}로 가리키는 인라인 그림이다.
 */
public record EmailMessage(String subject, String body, String html, List<InlineImage> images) {

    /** 인라인 그림 한 장. {@code contentId}는 꺾쇠 없이 {@code cid:} 뒤에 오는 값이다. */
    public record InlineImage(String contentId, String contentType, byte[] data) {
    }

    public EmailMessage {
        images = images == null ? List.of() : List.copyOf(images);
    }

    public EmailMessage(String subject, String body) {
        this(subject, body, null, List.of());
    }

    public boolean hasHtml() {
        return html != null && !html.isBlank();
    }
}
