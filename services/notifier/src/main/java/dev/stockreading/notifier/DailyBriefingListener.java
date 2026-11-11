package dev.stockreading.notifier;

import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.notifier.event.DailyBriefingEnvelope;
import dev.stockreading.notifier.mail.DailyEmailComposer;
import dev.stockreading.notifier.mail.EmailMessage;
import dev.stockreading.notifier.mail.MailSenderPort;
import dev.stockreading.notifier.recipient.Recipient;
import dev.stockreading.notifier.recipient.RecipientPort;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * briefing.daily.generated -> 이벤트가 가리킨 멤버에게 이메일 한 통(판은 멤버마다 따로 만들어진다). 최소 한 번 처리: 발송이 성공한 뒤에만 오프셋을 커밋한다.
 * 데일리 메일은 하루 정해진 통이라 시간당 한도로 막지도, 한도에 세지도 않는다(급변동 메일 한도와 무관).
 */
@Component
class DailyBriefingListener {

    private static final Logger log = LoggerFactory.getLogger(DailyBriefingListener.class);
    private static final int MAX_REMEMBERED = 1_000;

    private final ObjectMapper mapper;
    private final DailyEmailComposer composer;
    private final MailSenderPort mailSender;
    private final RecipientPort recipients;

    // 건별 메일과 같은 메모리 기반 중복 방지(실행 중에만 유지). 키는 이벤트와 받는 사람의 조합이다.
    private final Set<String> sentDailies = ConcurrentHashMap.newKeySet();

    DailyBriefingListener(ObjectMapper mapper, DailyEmailComposer composer, MailSenderPort mailSender,
            RecipientPort recipients) {
        this.mapper = mapper;
        this.composer = composer;
        this.mailSender = mailSender;
        this.recipients = recipients;
    }

    @KafkaListener(topics = Topics.DAILY_BRIEFING_GENERATED, groupId = "notifier")
    void onDailyBriefing(String message) {
        DailyBriefingEnvelope daily;
        EmailMessage email;
        try {
            daily = mapper.readValue(message, DailyBriefingEnvelope.class);
            if (!hasRequiredFields(daily)) {
                log.error("skipping briefing.daily.generated message without required fields eventId={}",
                        daily.eventId());
                return;
            }
            email = composer.compose(daily);
        } catch (JacksonException error) {
            // 형식이 잘못된 메시지는 재시도해도 유효해지지 않아서, 재시도하면 같은 실패만 반복한다.
            log.error("skipping malformed briefing.daily.generated message: {}", error.getOriginalMessage());
            return;
        } catch (DateTimeParseException error) {
            log.error("skipping briefing.daily.generated message with invalid rangeEnd: {}", error.getMessage());
            return;
        }

        // 받는 사람은 이벤트의 멤버 번호로 보내기 직전에 찾는다. 사용 중이 아니거나 그 판 수신을 껐으면 보내지 않는다(처리는 끝난 것으로 본다).
        List<Recipient> targets = recipients.forMember(daily.payload().memberId(), daily.payload().edition());
        if (targets.isEmpty()) {
            log.warn("no recipient for daily briefing; not emailed eventId={} memberId={} edition={}", daily.eventId(),
                    daily.payload().memberId(), daily.payload().edition());
            return;
        }
        for (Recipient target : targets) {
            String key = daily.eventId() + ":" + target.mailId();
            if (sentDailies.contains(key)) {
                log.info("duplicate daily briefing ignored eventId={} mailId={}", daily.eventId(), target.mailId());
                continue;
            }
            mailSender.send(target.address(), email); // 실패하면 예외를 던진다 -> Kafka 에러 핸들러가 재시도한다

            if (sentDailies.size() >= MAX_REMEMBERED) {
                sentDailies.clear();
            }
            sentDailies.add(key);
        }
    }

    private static boolean hasRequiredFields(DailyBriefingEnvelope daily) {
        DailyBriefingEnvelope.Payload payload = daily.payload();
        return daily.eventId() != null && payload != null
                && payload.memberId() != null && payload.edition() != null && payload.rangeEnd() != null
                && payload.text() != null && !payload.text().isBlank();
    }
}
