package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import tools.jackson.databind.json.JsonMapper;

// 저장해 둔 실제 시세 원문을 검사에 다시 넣어, 버리는 행이 분석으로 정한 목록과 같은지 본다.
// 평소에는 건너뛰고 PRICE_CHECK_REPLAY_INPUT·PRICE_CHECK_REPLAY_EXPECTED 가 있을 때만 돈다.
// 칸 이름은 수집기와 같은 BROKER_PRICE_CHECK_* 환경변수를 application.yml 로 묶어 받는다.
// 실제 값이 저장소와 출력에 남지 않도록 저장소 안 경로는 거절하고, 차이는 건수만 출력한다.
@EnabledIfEnvironmentVariable(named = "PRICE_CHECK_REPLAY_INPUT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "PRICE_CHECK_REPLAY_EXPECTED", matches = ".+")
class PriceCheckReplayTest {

    @Configuration
    @EnableConfigurationProperties(PriceCheckProperties.class)
    static class Wiring {
    }

    @Test
    void droppedEventIdsMatchTheExpectedList() {
        Path workingDir = Path.of("");
        Path input = outsideRepo(Path.of(System.getenv("PRICE_CHECK_REPLAY_INPUT")), workingDir);
        Path expectedFile = outsideRepo(Path.of(System.getenv("PRICE_CHECK_REPLAY_EXPECTED")), workingDir);

        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .run(context -> {
                    PriceCheckProperties properties = context.getBean(PriceCheckProperties.class);
                    assertThat(properties.noFieldsConfigured()).as("BROKER_PRICE_CHECK_* 칸 이름이 비어 있음").isFalse();
                    PriceFieldCheck check = new PriceFieldCheck(properties, JsonMapper.builder().build());

                    Set<String> expected = expectedIds(expectedFile);
                    Replay replay = replay(check, input);
                    Set<String> onlyExpected = new HashSet<>(expected);
                    onlyExpected.removeAll(replay.dropped());
                    Set<String> onlyDropped = new HashSet<>(replay.dropped());
                    onlyDropped.removeAll(expected);

                    System.out.println("price check replay: rows=" + replay.rows() + " dropped=" + replay.dropped().size()
                            + " expected=" + expected.size() + " onlyExpected=" + onlyExpected.size()
                            + " onlyDropped=" + onlyDropped.size());
                    assertThat(replay.rows()).as("입력 행 수").isPositive();
                    assertThat(onlyExpected.size() + onlyDropped.size()).as("버린 목록과 기대 목록의 차이 건수").isZero();
                });
    }

    @Test
    void pathsInsideTheRepositoryAreRejected() {
        Path workingDir = Path.of("");

        assertThatThrownBy(() -> outsideRepo(Path.of("build.gradle.kts"), workingDir))
                .isInstanceOf(IllegalArgumentException.class);
    }

    record Replay(int rows, Set<String> dropped) {
    }

    // 입력은 한 줄에 eventId<TAB>price<TAB>raw JSON 이다. 수백 MB 라 한 줄씩 읽는다.
    static Replay replay(PriceFieldCheck check, Path input) throws IOException {
        int rows = 0;
        Set<String> dropped = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\t", 3);
                if (parts.length < 3) {
                    throw new IllegalArgumentException("입력 " + (rows + 1) + "번째 행의 칸 수가 3이 아닙니다");
                }
                rows++;
                if (!check.failedChecks(Long.parseLong(parts[1].trim()), parts[2]).isEmpty()) {
                    dropped.add(parts[0].trim());
                }
            }
        }
        return new Replay(rows, dropped);
    }

    // 한 줄에 eventId 하나인 파일, 또는 첫 칸이 snapshot_event_id 인 분석 결과 CSV 를 받는다.
    static Set<String> expectedIds(Path file) throws IOException {
        Set<String> ids = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String id = line.split(",", 2)[0].trim();
                if (!id.isEmpty() && !id.equals("snapshot_event_id")) {
                    ids.add(id);
                }
            }
        }
        return ids;
    }

    // 실제 데이터 파일이 저장소에 들어가지 않도록 저장소 안의 경로는 읽기 전에 거절한다.
    static Path outsideRepo(Path path, Path workingDir) {
        Path target = path.toAbsolutePath().normalize();
        Path root = repoRoot(workingDir.toAbsolutePath().normalize());
        if (root != null && target.startsWith(root)) {
            throw new IllegalArgumentException("입력·기대 파일은 저장소 밖 경로로 지정해야 합니다");
        }
        return target;
    }

    private static Path repoRoot(Path dir) {
        for (Path p = dir; p != null; p = p.getParent()) {
            if (Files.exists(p.resolve(".git"))) {
                return p;
            }
        }
        return null;
    }
}
