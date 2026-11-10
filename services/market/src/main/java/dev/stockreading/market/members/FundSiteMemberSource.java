package dev.stockreading.market.members;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 운용사 사이트가 구성 종목(PDF) 화면에 쓰는 JSON을 읽는 어댑터. 요청 한 번에 ETF 한 개의 전체 구성이 온다.
 * 문서화된 API가 아니라 모양이 바뀔 수 있으므로, 모양이 다르면 일부만 읽지 않고 실패로 던져 호출한 쪽이 이전 구성을 유지하게 한다.
 *
 * <p>주식만 거르는 기준은 "비중이 있고 종목코드가 주식 코드 모양"이다. 현금·예금 행은 비중이 비어 있고, 채권·파생 등은
 * 코드가 길어 걸러진다. 시장은 코드가 숫자로 시작하는 6자리면 KR, 미국 티커 모양(영문 대문자 1~5자, 종류 구분 -/. 1자)이면
 * US로 본다. 그 밖(숫자 4자리 등 다른 해외 코드)은 해외 일봉·공시 조회 대상이 될 수 없으므로 후보에서 뺀다.
 */
@Component
class FundSiteMemberSource implements MemberSourcePort {

    private static final Logger log = LoggerFactory.getLogger(FundSiteMemberSource.class);

    // 응답이 비정상적으로 크면 읽지 않는다.
    private static final int MAX_BODY_CHARS = 2_000_000;
    private static final int MAX_NAME_CHARS = 100;
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    private static final Pattern STOCK_CODE = Pattern.compile("^[0-9A-Z]{1,6}([.-][0-9A-Z]{1,2})?$");
    private static final Pattern US_TICKER = Pattern.compile("^[A-Z]{1,5}([.-][A-Z])?$");
    private static final Pattern KR_CODE = Pattern.compile("^[0-9][0-9A-Z]{5}$");
    // 출처는 해외 종목코드에 거래소 꼬리를 붙여 준다(예: "<티커> US Equity", 종류 구분은 "/", 2026-11-10 실제 응답).
    private static final Pattern US_EQUITY = Pattern.compile("^(\\S+) US Equity$");
    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);

    private final MemberSourceProperties properties;
    private final ObjectMapper mapper;
    private final HttpClient client;

    FundSiteMemberSource(MemberSourceProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
        this.client = HttpClient.newBuilder().connectTimeout(properties.timeout()).build();
    }

    @Override
    public List<Member> fetchTop(String fundId, LocalDate baseDate, RawRecorder recorder) {
        String url = properties.urlTemplate().replace("{fundId}", fundId).replace("{date}", DATE.format(baseDate));
        HttpResponse<String> response;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(properties.timeout())
                    .header("User-Agent", properties.userAgent()).header("Accept", "application/json").GET().build();
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new MemberSourceException("request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MemberSourceException("request interrupted", e);
        } catch (IllegalArgumentException e) {
            // 설정한 주소 모양이 틀린 경우. 메시지에 주소가 섞이지 않게 원인은 붙이지 않는다.
            throw new MemberSourceException("invalid url template");
        }
        if (response.body().length() > MAX_BODY_CHARS) {
            throw new MemberSourceException("response too large");
        }
        // 200이 아니거나 모양이 달라 실패하는 응답도 원인을 볼 수 있게 해석 전에 원문을 남긴다.
        recorder.record(response.statusCode(), response.body());
        if (response.statusCode() != 200) {
            throw new MemberSourceException("unexpected status " + response.statusCode());
        }
        return parse(response.body(), properties.topCount());
    }

    /** 응답 본문에서 주식 행을 비중 내림차순(같으면 코드순)으로 topCount개 골라 순위를 붙인다. */
    List<Member> parse(String body, int topCount) {
        JsonNode rows;
        try {
            rows = mapper.readTree(body).path("pdf").path("list");
        } catch (JacksonException e) {
            throw new MemberSourceException("response is not json");
        }
        if (!rows.isArray()) {
            throw new MemberSourceException("unexpected shape");
        }
        // 같은 종목코드가 두 행 이상 오면 한 종목이 여러 묶음으로 나뉜 것으로 보고 비중을 합친다.
        // 합치지 않으면 한 종목이 상위 10개에 두 번 들어가 etf_member의 (etf, 종목) 키가 깨진다.
        Map<String, Candidate> byCode = new LinkedHashMap<>();
        int skippedMarket = 0;
        for (JsonNode row : rows) {
            Candidate candidate = candidate(row);
            if (candidate == null) {
                continue;
            }
            if (candidate.market() == null) {
                skippedMarket++;
                continue;
            }
            byCode.merge(candidate.market() + ":" + candidate.symbol(), candidate, Candidate::plus);
        }
        if (skippedMarket > 0) {
            log.info("member rows skipped for unsupported market count={}", skippedMarket);
        }
        List<Candidate> candidates = new ArrayList<>(byCode.values());
        if (candidates.isEmpty()) {
            throw new MemberSourceException("no stock rows");
        }
        candidates.sort(Comparator.comparing(Candidate::ratio).reversed().thenComparing(Candidate::symbol));
        List<Member> members = new ArrayList<>();
        for (Candidate c : candidates.subList(0, Math.min(topCount, candidates.size()))) {
            members.add(new Member(c.market(), c.symbol(), c.name(), c.weight(), members.size() + 1));
        }
        return members;
    }

    /** 주식 행이 아니면 null. 비중이 숫자가 아니거나 0~100을 벗어나면 모양이 다른 응답이라 실패로 본다. */
    private static Candidate candidate(JsonNode row) {
        String weight = text(row, "ratio");
        String code = withoutUsSuffix(text(row, "itmNo"));
        if (weight == null || code == null || !STOCK_CODE.matcher(code).matches()) {
            return null;
        }
        BigDecimal ratio;
        try {
            ratio = new BigDecimal(weight);
        } catch (NumberFormatException e) {
            throw new MemberSourceException("unexpected weight value");
        }
        if (ratio.signum() < 0 || ratio.compareTo(HUNDRED) > 0) {
            throw new MemberSourceException("unexpected weight value");
        }
        if (ratio.signum() == 0) {
            return null;
        }
        String market = KR_CODE.matcher(code).matches() ? "KR" : US_TICKER.matcher(code).matches() ? "US" : null;
        String name = text(row, "secNm");
        // 종목 테이블의 이름 칸 길이(100자)를 넘으면 저장이 실패해 그 ETF가 계속 갱신되지 않으므로 자른다.
        if (name != null && name.length() > MAX_NAME_CHARS) {
            name = name.substring(0, MAX_NAME_CHARS);
        }
        return new Candidate(market, code, name, weight, ratio);
    }

    /** 미국 꼬리가 붙은 코드만 티커로 바꾼다. 다른 거래소 꼬리는 공백이 남아 주식 코드 모양에서 걸러진다. */
    private static String withoutUsSuffix(String code) {
        if (code == null) {
            return null;
        }
        Matcher us = US_EQUITY.matcher(code);
        return us.matches() ? us.group(1).replace('/', '-') : code;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!(value.isString() || value.isNumber()) || value.asString().isBlank()) {
            return null;
        }
        return value.asString().strip();
    }

    // market이 null이면 지원하지 않는 시장의 코드라 후보에서 뺀다.
    private record Candidate(String market, String symbol, String name, String weight, BigDecimal ratio) {

        Candidate plus(Candidate other) {
            BigDecimal sum = ratio.add(other.ratio);
            return new Candidate(market, symbol, name != null ? name : other.name, sum.toPlainString(), sum);
        }
    }
}
