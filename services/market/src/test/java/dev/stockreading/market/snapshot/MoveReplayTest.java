package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import dev.stockreading.market.MarketProperties;
import tools.jackson.databind.json.JsonMapper;

// 저장해 둔 실제 시세를 실제 MoveDetector·MoveTracker 에 차례로 넣어, market.move 가 유형별로 몇 건 나오는지 센다.
// 평소에는 건너뛰고 MOVE_REPLAY_INPUT 이 있을 때만 돈다. MOVE_REPLAY_EXCLUDE 를 주면 그 목록의 행을 수집기가
// 버린 것으로 보고 저장·평가에서 뺀다. 설정은 application.yml 과 MARKET_* 환경변수로 받는다.
// 실제 값이 저장소와 출력에 남지 않도록 저장소 안 경로는 거절하고, 결과는 건수만 출력한다.
@EnabledIfEnvironmentVariable(named = "MOVE_REPLAY_INPUT", matches = ".+")
class MoveReplayTest {

    @Configuration
    @EnableConfigurationProperties(MarketProperties.class)
    static class Wiring {
    }

    @Test
    void countsMovesByType() throws IOException {
        Path workingDir = Path.of("");
        Path input = outsideRepo(Path.of(System.getenv("MOVE_REPLAY_INPUT")), workingDir);
        String excludeEnv = System.getenv("MOVE_REPLAY_EXCLUDE");
        Set<String> exclude = new HashSet<>();
        if (excludeEnv != null && !excludeEnv.isBlank()) {
            exclude = excludedIds(outsideRepo(Path.of(excludeEnv), workingDir));
        }
        Set<String> excluded = exclude;

        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .run(context -> {
                    MarketProperties properties = context.getBean(MarketProperties.class);
                    Result result = replay(properties, input, excluded);
                    System.out.println("move replay: prevCloseField=" + (isBlank(properties.prevCloseField()) ? "off" : "on")
                            + " rows=" + result.rows() + " excludedRows=" + result.excludedRows()
                            + " excludeList=" + excluded.size()
                            + " window=" + result.published().get(DetectionType.WINDOW)
                            + " prevClose=" + result.published().get(DetectionType.PREV_CLOSE)
                            + " prevCloseSymbols=" + result.prevCloseSymbols()
                            + " bothOnSameSnapshot=" + result.bothOnSameSnapshot()
                            + " prevCloseUnknown=" + result.prevCloseUnknown());
                    assertThat(result.rows()).as("입력 행 수").isPositive();
                });
    }

    @Test
    void pathsInsideTheRepositoryAreRejected() {
        assertThatThrownBy(() -> outsideRepo(Path.of("build.gradle.kts"), Path.of("")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    record Result(int rows, int excludedRows, Map<DetectionType, Integer> published, int prevCloseSymbols,
                  int bothOnSameSnapshot, int prevCloseUnknown) {
    }

    // 입력은 한 줄에 eventId<TAB>symbol<TAB>asOf(ISO)<TAB>price<TAB>raw JSON 이고, 시세처리기가 받은 순서다.
    static Result replay(MarketProperties properties, Path input, Set<String> exclude) throws IOException {
        // 시세 저장소 대역: 수집기가 보낸(빼지 않은) 행만 기억해 유형 1의 최근 이력으로 준다.
        Map<String, TreeMap<OffsetDateTime, List<PricePoint>>> history = new HashMap<>();
        PriceSnapshotRepository snapshots = mock(PriceSnapshotRepository.class);
        when(snapshots.findBetween(anyString(), any(OffsetDateTime.class), any(OffsetDateTime.class))).thenAnswer(inv -> {
            TreeMap<OffsetDateTime, List<PricePoint>> points = history.get(inv.<String>getArgument(0));
            List<PricePoint> found = new ArrayList<>();
            if (points != null) {
                points.subMap(inv.getArgument(1), true, inv.getArgument(2), false).values().forEach(found::addAll);
            }
            return found;
        });
        // 재발행 상태 대역. 시작할 때는 비어 있다(실제로는 범위 앞의 상태가 남아 있었을 수 있다).
        Map<String, MoveState> stored = new HashMap<>();
        MoveStateRepository states = mock(MoveStateRepository.class);
        when(states.find(anyString(), any(DetectionType.class)))
                .thenAnswer(inv -> Optional.ofNullable(stored.get(key(inv.getArgument(0), inv.getArgument(1)))));
        doAnswer(inv -> {
            stored.put(key(inv.getArgument(0), inv.getArgument(1)), new MoveState(inv.getArgument(2), inv.getArgument(3)));
            return null;
        }).when(states).save(anyString(), any(DetectionType.class), anyLong(), any(OffsetDateTime.class), any(OffsetDateTime.class));
        doAnswer(inv -> {
            stored.remove(key(inv.getArgument(0), inv.getArgument(1)));
            return null;
        }).when(states).delete(anyString(), any(DetectionType.class));

        Map<DetectionType, Integer> published = new EnumMap<>(DetectionType.class);
        for (DetectionType type : DetectionType.values()) {
            published.put(type, 0);
        }
        Set<String> prevCloseSymbols = new HashSet<>();
        List<DetectionType> current = new ArrayList<>();
        MovePublisher publisher = mock(MovePublisher.class);
        doAnswer(inv -> {
            Detection detection = inv.getArgument(1);
            published.merge(detection.type(), 1, Integer::sum);
            current.add(detection.type());
            if (detection.type() == DetectionType.PREV_CLOSE) {
                prevCloseSymbols.add(inv.getArgument(0));
            }
            return null;
        }).when(publisher).publish(anyString(), any(Detection.class), any(OffsetDateTime.class), any(OffsetDateTime.class), any());

        MoveDetector detector = new MoveDetector(snapshots, properties, JsonMapper.builder().build());
        MoveTracker tracker = new MoveTracker(detector, states, publisher);

        int rows = 0;
        int excludedRows = 0;
        int both = 0;
        int prevCloseUnknown = 0;
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\t", 5);
                if (parts.length < 5) {
                    throw new IllegalArgumentException("입력 " + (rows + 1) + "번째 행의 칸 수가 5가 아닙니다");
                }
                rows++;
                if (exclude.contains(parts[0].trim())) {
                    excludedRows++;
                    continue;
                }
                String symbol = parts[1].trim();
                OffsetDateTime asOf = OffsetDateTime.parse(parts[2].trim());
                long price = Long.parseLong(parts[3].trim());
                current.clear();
                // 실제 리스너처럼 평가 전에 저장한다. 유형 1 조회는 현재 시각 미만만 보므로 결과는 같다.
                history.computeIfAbsent(symbol, s -> new TreeMap<>())
                        .computeIfAbsent(asOf, t -> new ArrayList<>()).add(new PricePoint(asOf, price));
                tracker.onSnapshot(symbol, asOf, price, parts[4], "replay");
                if (current.contains(DetectionType.WINDOW) && current.contains(DetectionType.PREV_CLOSE)) {
                    both++;
                }
                if (detector.evaluate(symbol, asOf, price, parts[4]).stream()
                        .anyMatch(e -> e.type() == DetectionType.PREV_CLOSE && e.outcome() == Evaluation.Outcome.UNKNOWN)) {
                    prevCloseUnknown++;
                }
            }
        }
        return new Result(rows, excludedRows, published, prevCloseSymbols.size(), both, prevCloseUnknown);
    }

    private static String key(String symbol, DetectionType type) {
        return symbol + "/" + type;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    // 한 줄에 eventId 하나인 파일, 또는 첫 칸이 snapshot_event_id 인 분석 결과 CSV 를 받는다.
    static Set<String> excludedIds(Path file) throws IOException {
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
            throw new IllegalArgumentException("입력·제외 파일은 저장소 밖 경로로 지정해야 합니다");
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
