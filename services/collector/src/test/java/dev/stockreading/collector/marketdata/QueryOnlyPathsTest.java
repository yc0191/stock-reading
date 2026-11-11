package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

/**
 * 시세 제공자 API는 조회(토큰 발급, 시세 조회)만 호출한다(ADR-007). 주문·계좌처럼 조회용 목록 밖의 경로가 코드에 생기면 실패한다.
 * 소스의 경로 문자열과, 로컬 가짜 서버로 실제 호출된 경로를 모두 목록과 비교한다. 실제 서버는 호출하지 않는다.
 */
class QueryOnlyPathsTest {

    // 새 조회 API를 쓰게 될 때만 여기에 더한다. 주문·계좌 경로는 넣지 않는다.
    private static final Set<String> QUERY_PATHS = Set.of(
            "/oauth2/token",      // 토큰 발급
            "/api/v1/ivu10140",   // 현재가
            "/api/v1/ivs11560",   // 국내 일봉
            "/api/v1/gsc10060",   // 해외 일봉
            "/api/v1/iva60140",   // 세계지수
            "/api/v1/iva60190",   // 환율종합
            "/api/v1/ivsa0070",   // 시장종합
            FakeBrokerServer.MARKET_STATUS_PATH);  // 장 운영 상태(실제 경로는 설정으로 받는다)

    private static final Path MAIN_SOURCES = Path.of("src/main/java/dev/stockreading/collector");
    private static final Path CLIENT_PACKAGE = MAIN_SOURCES.resolve("marketdata");
    private static final Pattern PATH_LITERAL = Pattern.compile("\"(/[^\"\\s]*)\"");
    private static final Pattern POST_CALL = Pattern.compile("\\bpost(?:Raw)?\\(\\s*([^,)]*)");
    private static final String REQUEST_URI = "URI.create(properties.baseUrl() + path)";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T01:00:00Z"), ZoneId.of("Asia/Seoul"));

    @Test
    void everyPathStringInTheClientPackageIsAQueryPath() throws IOException {
        Set<String> found = pathLiterals(read(CLIENT_PACKAGE));

        assertThat(found).as("시세 클라이언트 패키지의 경로 문자열").isNotEmpty().isSubsetOf(QUERY_PATHS);
    }

    @Test
    void requestsAreBuiltOnlyFromTheBaseUrlAndALiteralPath() throws IOException {
        String source = read(CLIENT_PACKAGE);

        // 요청 주소는 post/postRaw 안에서만 만들고, 두 메서드에는 경로를 문자열 그대로만 넘긴다.
        // 그래야 위의 경로 문자열 비교가 호출 가능한 경로 전부를 덮는다.
        assertThat(count(source, "URI.create(")).isEqualTo(count(source, REQUEST_URI)).isEqualTo(2);
        Matcher call = POST_CALL.matcher(source);
        while (call.find()) {
            String argument = call.group(1).trim();
            if (argument.isEmpty() || argument.startsWith("String path")) {
                continue; // 주석의 "post()" 와 메서드 선언
            }
            assertThat(argument).as("post/postRaw 에 넘기는 경로").matches("\"/[^\"]*\"");
        }
    }

    @Test
    void onlyTheClientPackageUsesTheProviderSettings() throws IOException {
        try (Stream<Path> files = Files.walk(MAIN_SOURCES)) {
            List<Path> outside = files
                    .filter(f -> f.toString().endsWith(".java") && !f.startsWith(CLIENT_PACKAGE))
                    .filter(f -> readQuietly(f).contains("BrokerProperties"))
                    .toList();
            assertThat(outside).as("시세 제공자 설정(주소·키)을 쓰는 다른 패키지").isEmpty();
        }
    }

    @Test
    void everyClientMethodCallsOnlyQueryPathsOnALocalServer() throws Exception {
        List<String> called = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            called.add(path);
            String body = path.equals("/oauth2/token")
                    ? "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER
                            + ",\"dataBody\":{\"access_token\":\"tok-test\",\"token_type\":\"Bearer\",\"expires_in\":86400}}"
                    : "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{\"now_prc\":\"10250\"}}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            BrokerClient client = new BrokerClient(
                    new BrokerProperties("http://127.0.0.1:" + server.getAddress().getPort(), "key-test", "secret-test", 5, 10),
                    JsonMapper.builder().build(), CLOCK, CallRateLimiter.perSecond(10));
            // 새로 생긴 메서드도 빠지지 않도록 선언된 메서드를 모두 부른다.
            for (Method method : BrokerClient.class.getDeclaredMethods()) {
                if (Modifier.isPrivate(method.getModifiers()) || Modifier.isStatic(method.getModifiers()) || method.isSynthetic()) {
                    continue;
                }
                method.setAccessible(true);
                try {
                    method.invoke(client, arguments(method));
                } catch (InvocationTargetException e) {
                    // 응답 해석 실패는 상관없다. 어떤 경로를 불렀는지만 본다.
                }
            }
        } finally {
            server.stop(0);
        }

        assertThat(new TreeSet<>(called)).as("가짜 서버가 받은 경로").isNotEmpty().isSubsetOf(QUERY_PATHS);
    }

    @Test
    void anOrderPathInSourceIsCaught() {
        String source = "String orderRaw() { return postRaw(\"/api/v1/order-cash\", body); }";

        assertThat(pathLiterals(source)).containsExactly("/api/v1/order-cash").doesNotContainAnyElementsOf(QUERY_PATHS);
    }

    private static Object[] arguments(Method method) {
        Class<?>[] types = method.getParameterTypes();
        Object[] values = new Object[types.length];
        for (int i = 0; i < types.length; i++) {
            if (types[i] == String.class && method.getName().equals("marketStatusRaw")) {
                values[i] = FakeBrokerServer.MARKET_STATUS_PATH; // 경로는 설정으로 받으므로 가짜 경로를 넣는다
            } else if (types[i] == java.time.Duration.class) {
                values[i] = java.time.Duration.ofSeconds(5);
            } else if (types[i] == String.class) {
                values[i] = "TEST0001";
            } else if (types[i] == int.class || types[i] == Integer.class) {
                values[i] = 1;
            } else if (types[i] == long.class || types[i] == Long.class) {
                values[i] = 1L;
            } else if (types[i] == boolean.class || types[i] == Boolean.class) {
                values[i] = false;
            } else {
                fail(method.getName() + " 의 인자 형식 " + types[i].getSimpleName() + " 에 넣을 시험 값을 arguments() 에 더하세요");
            }
        }
        return values;
    }

    private static Set<String> pathLiterals(String source) {
        Set<String> found = new TreeSet<>();
        Matcher m = PATH_LITERAL.matcher(source);
        while (m.find()) {
            found.add(m.group(1));
        }
        return found;
    }

    private static String read(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            StringBuilder all = new StringBuilder();
            files.filter(f -> f.toString().endsWith(".java")).sorted().forEach(f -> all.append(readQuietly(f)).append('\n'));
            return all.toString();
        }
    }

    private static String readQuietly(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new IllegalStateException(file.toString(), e);
        }
    }

    private static int count(String text, String part) {
        int n = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + 1)) {
            n++;
        }
        return n;
    }
}
