package dev.stockreading.collector.edgar;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.JsonNode;

/**
 * submissions 응답에서 공시 메타데이터를 뽑는다. filings.recent 는 같은 위치끼리 한 건을 이루는 배열 묶음이다.
 */
final class SubmissionsParser {

    private static final Logger log = LoggerFactory.getLogger(SubmissionsParser.class);
    static final Pattern ACCESSION = Pattern.compile("[0-9]{10}-[0-9]{2}-[0-9]{6}");
    private static final List<String> COLUMNS =
            List.of("accessionNumber", "filingDate", "acceptanceDateTime", "form", "items", "primaryDocument");

    private SubmissionsParser() {
    }

    static CompanySubmissions parse(JsonNode body, String cik, String archivesBaseUrl) {
        JsonNode recent = body.path("filings").path("recent");
        if (!recent.isObject()) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, null);
        }
        int size = recent.path("accessionNumber").size();
        for (String column : COLUMNS) {
            // 배열 길이가 다르면 위치로 짝을 맞출 수 없어 다른 공시의 값이 섞인다.
            if (!recent.path(column).isArray() || recent.path(column).size() != size) {
                throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, null);
            }
        }
        List<Filing> filings = new ArrayList<>();
        int skipped = 0;
        for (int i = 0; i < size; i++) {
            Filing filing = toFiling(recent, i, cik, archivesBaseUrl);
            if (filing == null) {
                skipped++;
            } else {
                filings.add(filing);
            }
        }
        if (skipped > 0) {
            log.warn("edgar filings skipped (malformed row) count={}", skipped);
        }
        return new CompanySubmissions(cik, exchanges(body.path("exchanges")), filings);
    }

    private static Filing toFiling(JsonNode recent, int i, String cik, String archivesBaseUrl) {
        String accession = text(recent, "accessionNumber", i);
        String form = text(recent, "form", i);
        OffsetDateTime acceptedAt = acceptedAt(text(recent, "acceptanceDateTime", i));
        LocalDate filingDate = date(text(recent, "filingDate", i));
        if (!ACCESSION.matcher(accession).matches() || form.isEmpty() || acceptedAt == null || filingDate == null) {
            return null;
        }
        return new Filing(accession, form, acceptedAt, filingDate, items(text(recent, "items", i)),
                documentUrl(archivesBaseUrl, cik, accession, text(recent, "primaryDocument", i)));
    }

    // SEC 는 밀리초까지 주지만 값은 항상 0이다. 시간 규칙 비교를 단순하게 하려고 초 단위 UTC 로 맞춘다.
    static OffsetDateTime acceptedAt(String raw) {
        try {
            return Instant.parse(raw).truncatedTo(ChronoUnit.SECONDS).atOffset(ZoneOffset.UTC);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static LocalDate date(String raw) {
        try {
            return LocalDate.parse(raw);
        } catch (RuntimeException e) {
            return null;
        }
    }

    // "2.02,9.01" 형식. 6-K 처럼 항목이 없는 형식은 빈 문자열이라 빈 목록이 된다.
    static List<String> items(String raw) {
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .distinct()
                .toList();
    }

    // 원문 경로: <archives>/<0을 뗀 CIK>/<하이픈을 뗀 접수번호>/<주 문서>. 주 문서가 없으면 접수 색인 페이지를 쓴다.
    static String documentUrl(String archivesBaseUrl, String cik, String accession, String primaryDocument) {
        String folder = archivesBaseUrl + "/" + Long.parseLong(cik) + "/" + accession.replace("-", "");
        return primaryDocument.isEmpty() ? folder + "/" + accession + "-index.htm" : folder + "/" + primaryDocument;
    }

    private static List<String> exchanges(JsonNode node) {
        List<String> result = new ArrayList<>();
        for (JsonNode exchange : node) {
            String value = exchange.asString("").trim();
            if (!value.isEmpty()) {
                result.add(value);
            }
        }
        return result;
    }

    private static String text(JsonNode recent, String column, int i) {
        JsonNode value = recent.path(column).get(i);
        return value == null || value.isNull() ? "" : value.asString("").trim();
    }
}
