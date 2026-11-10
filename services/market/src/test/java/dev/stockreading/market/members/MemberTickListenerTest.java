package dev.stockreading.market.members;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;

import tools.jackson.databind.json.JsonMapper;

class MemberTickListenerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-09T22:30:00Z"), ZoneId.of("Asia/Seoul"));

    private MemberFetchService service;
    private MemberTickListener listener;

    @BeforeEach
    void setUp() {
        service = mock(MemberFetchService.class);
        listener = new MemberTickListener(service, JsonMapper.builder().build(), CLOCK);
    }

    private static String tick(String jobType, String scheduledFor) {
        return """
                {"eventId": "a3f7c9d1-2b4e-4c68-9e05-1d7b8a6c3f42", "eventType": "schedule.tick", "schemaVersion": 1,
                 "occurredAt": "2026-11-10T07:30:00+09:00", "producedAt": "2026-11-10T07:30:00.100+09:00",
                 "correlationId": "c", "futureField": 1,
                 "payload": {"jobType": "%s", "scheduledFor": "%s", "extra": true}}
                """.formatted(jobType, scheduledFor);
    }

    @Test
    void aMembersTickRunsTheFetchForTodayInKoreaTime() {
        listener.onTick(tick("collect-members", "2026-11-10T07:30:00+09:00"));

        verify(service).run(LocalDate.parse("2026-11-10"));
    }

    @Test
    void otherJobTypesAreIgnored() {
        listener.onTick(tick("collect-pre-open", "2026-11-10T07:30:00+09:00"));

        verify(service, never()).run(any());
    }

    @Test
    void theSameTickTwiceRunsOnlyOnce() {
        listener.onTick(tick("collect-members", "2026-11-10T07:30:00+09:00"));
        listener.onTick(tick("collect-members", "2026-11-10T07:30:00+09:00"));
        listener.onTick(tick("collect-members", "2026-11-11T07:30:00+09:00"));

        verify(service, times(2)).run(any());
    }

    @Test
    void usesItsOwnConsumerGroupBecauseTheTickTopicHasOnePartition() throws NoSuchMethodException {
        KafkaListener annotation = MemberTickListener.class.getDeclaredMethod("onTick", String.class)
                .getAnnotation(KafkaListener.class);

        org.assertj.core.api.Assertions.assertThat(annotation.groupId()).isEqualTo("market-members");
        org.assertj.core.api.Assertions.assertThat(annotation.topics()).containsExactly("schedule.tick");
    }
}
