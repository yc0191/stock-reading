package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.notifier.mail.MailSenderPort;
import dev.stockreading.notifier.recipient.Recipient;
import dev.stockreading.notifier.recipient.RecipientPort;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 가상 흐름: 실제 배선(메일 패키지 전체)에 설정 없이 데일리 메시지를 넣으면 드라이런 로그로만 나가고 주소는 가려진다. */
class DailyBriefingDryRunTest {

    // 드라이런 발송기는 메일 패키지 안에서만 보이므로 로거 이름으로 붙는다.
    private static final String DRY_RUN_LOGGER = "dev.stockreading.notifier.mail.LoggingMailSender";

    @Configuration
    @EnableConfigurationProperties(NotifierProperties.class)
    @ComponentScan(basePackageClasses = MailSenderPort.class)
    @Import(DailyBriefingListener.class)
    static class Wiring {
        @Bean
        ObjectMapper objectMapper() {
            return JsonMapper.builder().build();
        }

        @Bean
        RecipientPort recipientPort() {
            RecipientPort port = mock(RecipientPort.class);
            when(port.forEdition(any())).thenReturn(List.of(new Recipient(1, "reader@example.com")));
            return port;
        }

        @Bean
        Clock clock() {
            return new TestClock(Instant.parse("2026-10-20T08:00:00Z"));
        }
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(Wiring.class)
            .withPropertyValues("notifier.subject-prefix=[시뮬레이션]");

    private ListAppender<ILoggingEvent> logs;

    @BeforeEach
    void attachLogs() {
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(DRY_RUN_LOGGER)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(DRY_RUN_LOGGER)).detachAppender(logs);
    }

    @Test
    void withNoSendSettingTheDailyEmailIsOnlyLoggedWithAMaskedAddress() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            context.getBean(DailyBriefingListener.class)
                    .onDailyBriefing(ContractSchemas.sample("briefing.daily.generated.sample.json"));
        });

        assertThat(logs.list).hasSize(1);
        String line = logs.list.get(0).getFormattedMessage();
        assertThat(line).startsWith("DRY-RUN (not sent) to=r****@example.com");
        assertThat(line).contains("subject=\"[시뮬레이션] 마감 후 판 데일리 브리핑 2026-10-20\"");
        assertThat(line).contains("자료 없음 항목: etf-daily-change:TEST02");
        assertThat(line).doesNotContain("reader@example.com");
        // HTML과 그림은 내용이 아니라 크기만 남는다
        assertThat(line).contains(" images=1[70B]").doesNotContain("iVBORw0KGgo").doesNotContain("<html>");
    }
}
