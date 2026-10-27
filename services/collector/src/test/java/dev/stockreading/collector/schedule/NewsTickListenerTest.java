package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.news.NewsCollector;
import dev.stockreading.collector.news.NewsItem;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class NewsTickListenerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));

    private NewsCollector collector;
    private KafkaTemplate<String, String> kafka;
    private NewsTickListener listener;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        collector = mock(NewsCollector.class);
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        ObjectMapper mapper = JsonMapper.builder().build();
        listener = new NewsTickListener(new NewsPublisher(collector, kafka, mapper, CLOCK), mapper);
    }

    @Test
    void newsTickPublishesOneEventPerItemKeyedByUrlAndPassesTheContract() throws IOException {
        when(collector.collect()).thenReturn(List.of(item("https://news.example.com/a"), item("https://news.example.com/b")));

        listener.onTick(newsTick());

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), key.capture(), value.capture());
        assertThat(key.getAllValues()).containsExactly("https://news.example.com/a", "https://news.example.com/b");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate("news.collected.v1.schema.json", json)).isEmpty();
            assertThat(json).contains("\"correlationId\":\"collect-news-2026-10-20T16:00:00+09:00\"");
        }
    }

    @Test
    void secondTickDoesNotRepublishTheSameArticles() throws IOException {
        when(collector.collect()).thenReturn(List.of(item("https://news.example.com/a")));

        listener.onTick(newsTick("2026-10-20T16:00:00+09:00"));
        listener.onTick(newsTick("2026-10-20T16:15:00+09:00"));

        verify(kafka, times(1)).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());
    }

    @Test
    void sameTickDeliveredTwiceCollectsOnlyOnce() throws IOException {
        when(collector.collect()).thenReturn(List.of());

        listener.onTick(newsTick());
        listener.onTick(newsTick());

        verify(collector, times(1)).collect();
    }

    @Test
    void listenerSurvivesCollectFailureAndWorksOnTheNextTick() throws IOException {
        when(collector.collect())
                .thenThrow(new IllegalStateException("boom"))
                .thenReturn(List.of(item("https://news.example.com/a")));

        assertThatCode(() -> listener.onTick(newsTick("2026-10-20T16:00:00+09:00"))).doesNotThrowAnyException();
        listener.onTick(newsTick("2026-10-20T16:15:00+09:00"));

        verify(kafka, times(1)).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());
    }

    @Test
    void ignoresOtherJobTypes() throws IOException {
        listener.onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        verify(collector, never()).collect();
        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void usesItsOwnConsumerGroupSoItDoesNotCompeteWithTickListener() throws NoSuchMethodException {
        KafkaListener news = NewsTickListener.class.getDeclaredMethod("onTick", String.class)
                .getAnnotation(KafkaListener.class);
        KafkaListener market = TickListener.class.getDeclaredMethod("onTick", String.class)
                .getAnnotation(KafkaListener.class);

        assertThat(news.groupId()).isNotEqualTo(market.groupId());
    }

    private static String newsTick() throws IOException {
        return newsTick("2026-10-20T16:00:00+09:00");
    }

    private static String newsTick(String scheduledFor) throws IOException {
        return ContractSchemas.sample("schedule.tick.sample.json")
                .replace("\"collect\"", "\"collect-news\"")
                .replace("\"correlationId\": \"collect-2026-10-20T16:00:00+09:00\"",
                        "\"correlationId\": \"collect-news-2026-10-20T16:00:00+09:00\"")
                .replace("\"scheduledFor\": \"2026-10-20T16:00:00+09:00\"", "\"scheduledFor\": \"" + scheduledFor + "\"");
    }

    private static NewsItem item(String url) {
        return new NewsItem("테스트 기사 제목", "테스트 기사 요약입니다.", url, null,
                OffsetDateTime.parse("2026-10-20T15:42:00+09:00"), "news.example.com",
                Set.of("TEST01"), Set.of("테스트검색어"));
    }
}
