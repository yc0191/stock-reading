package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mock.env.MockEnvironment;

/** .env에 아무것도 없을 때 지켜져야 하는 기본값을 지킨다: 안전함(드라이런)과 읽을 수 있음(한글 머리말). */
class ConfigDefaultsTest {

    private static MockEnvironment applicationYmlOnly() throws IOException {
        MockEnvironment environment = new MockEnvironment(); // OS 환경변수를 쓰지 않으므로 결과가 외부 환경에 흔들리지 않는다
        for (PropertySource<?> source : new YamlPropertySourceLoader()
                .load("application", new FileSystemResource("src/main/resources/application.yml"))) {
            environment.getPropertySources().addLast(source);
        }
        return environment;
    }

    @Test
    void sendingIsOffByDefault() throws IOException {
        assertThat(applicationYmlOnly().getProperty("notifier.send-enabled")).isEqualTo("false");
    }

    @Test
    void theSubjectPrefixDefaultIsReadAsProperKorean() throws IOException {
        // 깨진 기본값은 실제 이메일 제목마다 붙게 된다.
        assertThat(applicationYmlOnly().getProperty("notifier.subject-prefix")).isEqualTo("[시뮬레이션]");
    }

    @Test
    void anEmptyPrefixVariableRemovesTheDefaultPrefix() throws IOException {
        // 운영은 실행 설정에서 이 값을 빈 값으로 넘겨 개발용 머리말을 떼므로, 빈 값이 기본값으로 바뀌면 안 된다.
        MockEnvironment environment = applicationYmlOnly();
        environment.getPropertySources().addFirst(new MapPropertySource("env", Map.of("NOTIFIER_SUBJECT_PREFIX", "")));
        assertThat(environment.getProperty("notifier.subject-prefix")).isEmpty();
    }
}
