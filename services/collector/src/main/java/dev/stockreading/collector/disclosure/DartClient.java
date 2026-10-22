package dev.stockreading.collector.disclosure;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
class DartClient implements DisclosurePort {

    static final String STATUS_OK = "000";
    static final String STATUS_NO_DATA = "013";
    private static final DateTimeFormatter DART_DATE = DateTimeFormatter.BASIC_ISO_DATE;

    private final DartProperties properties;
    private final ObjectMapper mapper;
    private final HttpClient http;

    DartClient(DartProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .build();
    }

    @Override
    public List<Disclosure> list(String companyCode, LocalDate from, LocalDate to) {
        List<Disclosure> result = new ArrayList<>();
        int page = 1;
        int totalPages;
        do {
            JsonNode body = get(companyCode, from, to, page);
            String status = body.path("status").asString("");
            // 조회 결과가 없는 것은 오류가 아니라 빈 목록이다.
            if (STATUS_NO_DATA.equals(status)) {
                return result;
            }
            if (!STATUS_OK.equals(status)) {
                throw new DartApiException("공시 API 오류 status=" + status + " message=" + body.path("message").asString(""));
            }
            for (JsonNode item : body.path("list")) {
                result.add(toDisclosure(item));
            }
            totalPages = body.path("total_page").asInt(1);
            page++;
        } while (page <= totalPages);
        return result;
    }

    private JsonNode get(String companyCode, LocalDate from, LocalDate to, int page) {
        if (properties.apiKey() == null || properties.apiKey().isBlank()) {
            throw new DartApiException("DART_API_KEY 가 설정되지 않았습니다");
        }
        StringBuilder query = new StringBuilder()
                .append("crtfc_key=").append(encode(properties.apiKey()))
                .append("&bgn_de=").append(from.format(DART_DATE))
                .append("&end_de=").append(to.format(DART_DATE))
                .append("&page_no=").append(page)
                .append("&page_count=").append(properties.pageCount());
        if (companyCode != null && !companyCode.isBlank()) {
            query.append("&corp_code=").append(encode(companyCode));
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(properties.baseUrl() + "/list.json?" + query))
                .timeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .GET()
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                throw new DartApiException("공시 API HTTP 오류 status=" + response.statusCode());
            }
            return mapper.readTree(response.body());
        } catch (IOException e) {
            // 원인 예외 메시지에 요청 URL(인증키)이 들어갈 수 있어 연결하지 않는다.
            throw new DartApiException("공시 API 호출 실패: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DartApiException("공시 API 호출 중단", e);
        }
    }

    private static Disclosure toDisclosure(JsonNode item) {
        return new Disclosure(
                item.path("corp_code").asString(""),
                item.path("corp_name").asString(""),
                item.path("stock_code").asString(""),
                item.path("corp_cls").asString(""),
                item.path("report_nm").asString(""),
                item.path("rcept_no").asString(""),
                item.path("flr_nm").asString(""),
                LocalDate.parse(item.path("rcept_dt").asString(""), DART_DATE),
                item.path("rm").asString(""));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
