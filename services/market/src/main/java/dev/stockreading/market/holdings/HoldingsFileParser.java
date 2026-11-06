package dev.stockreading.market.holdings;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 보유 종목 JSON 파일(종목 배열)을 읽어 검증한다. 항목마다 kind(ETF/STOCK), market, symbol, name, tracks(ETF만),
 * sector, relation(HELD/WATCH/NONE)이 있고, ETF는 members 배열(market, symbol, name, weight, rank)을 가진다.
 *
 * <p>ETF 안에만 나오는 종목은 relation NONE인 STOCK 종목으로 만들고, 같은 종목이 여러 곳에 나와도 종목은 한 줄이다.
 * 보유 종목이 드러나지 않도록 오류 메시지에는 값을 넣지 않고 몇 번째 항목(구성 종목은 몇 번째 구성 종목)인지와
 * 오류 종류만 적는다. 종류 구분 표기는 SEC와 같은 "-"로 맞춘다.
 */
final class HoldingsFileParser {

    private static final Pattern MARKET = Pattern.compile("[A-Z]{2,4}");
    private static final Pattern SYMBOL = Pattern.compile("[0-9A-Z]{1,12}([.-][0-9A-Z]{1,2})?");
    private static final Pattern ETF_CODE = Pattern.compile("[0-9A-Z]{6}");
    private static final int TEXT_MAX = 100;
    private static final Set<String> RELATIONS = Set.of("HELD", "WATCH", "NONE");

    private HoldingsFileParser() {
    }

    static HoldingsPlan parse(String json, ObjectMapper mapper) {
        if (json == null || json.isBlank()) {
            return HoldingsPlan.empty();
        }
        JsonNode root;
        try {
            root = mapper.readTree(json);
        } catch (RuntimeException e) {
            // 파싱 예외 메시지에 파일 내용이 섞일 수 있어 원인을 붙이지 않는다.
            throw new IllegalArgumentException("보유 종목 파일이 JSON 형식이 아닙니다");
        }
        if (!root.isArray()) {
            throw new IllegalArgumentException("보유 종목 파일의 맨 위는 종목 배열이어야 합니다");
        }
        Map<String, HoldingsPlan.Instrument> instruments = new LinkedHashMap<>();
        Set<String> declared = new HashSet<>();
        List<HoldingsPlan.Membership> memberships = new ArrayList<>();
        // 구성 종목에만 이름이 있고 최상위 항목에 이름이 없을 때 채워 넣을 이름
        Map<String, String> memberNames = new HashMap<>();

        for (int i = 0; i < root.size(); i++) {
            int position = i + 1;
            JsonNode item = root.get(i);
            if (!item.isObject()) {
                throw error("종목 " + position + "번째 항목", "객체가 아닙니다");
            }
            String where = "종목 " + position + "번째 항목";
            String kind = enumText(item, "kind", where, Set.of("ETF", "STOCK"));
            String market = market(item, where);
            String symbol = symbol(item, where);
            if (kind.equals("ETF") && market.equals("KR") && !ETF_CODE.matcher(symbol).matches()) {
                throw error(where, "국내 ETF 종목코드 형식 오류가 있습니다(6자리 숫자·대문자여야 합니다)");
            }
            String tracks = optionalText(item, "tracks", where);
            if (tracks != null && !kind.equals("ETF")) {
                throw error(where, "추종 대상(tracks)은 ETF에만 적을 수 있습니다");
            }
            String relation = item.path("relation").isMissingNode() || item.path("relation").isNull()
                    ? "NONE" : enumText(item, "relation", where, RELATIONS);
            String key = market + ":" + symbol;
            if (!declared.add(key)) {
                throw error(where, "같은 종목이 두 번 적혔습니다");
            }
            instruments.put(key, new HoldingsPlan.Instrument(kind, market, symbol, optionalText(item, "name", where),
                    tracks, optionalText(item, "sector", where), relation));

            JsonNode members = item.path("members");
            if (!members.isMissingNode() && !members.isNull()) {
                if (!kind.equals("ETF")) {
                    throw error(where, "구성 종목(members)은 ETF에만 적을 수 있습니다");
                }
                if (!members.isArray()) {
                    throw error(where, "구성 종목(members)이 배열이 아닙니다");
                }
                readMembers(members, market, symbol, where, instruments, memberNames, memberships);
            }
        }

        // 최상위 항목에 이름이 없으면 구성 종목에 적힌 이름으로 채운다. 구성 종목에만 나온 종목은 이 단계에서 이름을 얻는다.
        memberNames.forEach((key, name) -> {
            HoldingsPlan.Instrument found = instruments.get(key);
            if (found != null && found.name() == null) {
                instruments.put(key, new HoldingsPlan.Instrument(found.kind(), found.market(), found.symbol(), name,
                        found.tracks(), found.sector(), found.relation()));
            }
        });
        return new HoldingsPlan(new ArrayList<>(instruments.values()), memberships);
    }

    private static void readMembers(JsonNode members, String etfMarket, String etfSymbol, String etfWhere,
            Map<String, HoldingsPlan.Instrument> instruments, Map<String, String> memberNames,
            List<HoldingsPlan.Membership> memberships) {
        Set<String> seen = new HashSet<>();
        Set<Integer> ranks = new HashSet<>();
        for (int j = 0; j < members.size(); j++) {
            String where = etfWhere + "의 " + (j + 1) + "번째 구성 종목";
            JsonNode member = members.get(j);
            if (!member.isObject()) {
                throw error(where, "객체가 아닙니다");
            }
            String market = market(member, where);
            String symbol = symbol(member, where);
            String key = market + ":" + symbol;
            if (key.equals(etfMarket + ":" + etfSymbol)) {
                throw error(where, "ETF가 자기 자신을 담을 수 없습니다");
            }
            if (!seen.add(key)) {
                throw error(where, "같은 구성 종목이 두 번 적혔습니다");
            }
            String name = optionalText(member, "name", where);
            BigDecimal weight = weight(member, where);
            int rank = rank(member, j + 1, where);
            if (!ranks.add(rank)) {
                throw error(where, "비중 순위(rank)가 다른 구성 종목과 같습니다");
            }
            instruments.putIfAbsent(key, new HoldingsPlan.Instrument("STOCK", market, symbol, null, null, null, "NONE"));
            if (name != null) {
                memberNames.putIfAbsent(key, name);
            }
            memberships.add(new HoldingsPlan.Membership(etfMarket, etfSymbol, market, symbol, weight, rank));
        }
    }

    private static String enumText(JsonNode node, String field, String where, Set<String> allowed) {
        JsonNode value = node.path(field);
        if (!value.isString() || !allowed.contains(value.asString())) {
            throw error(where, field + " 값이 허용 목록에 없습니다");
        }
        return value.asString();
    }

    private static String market(JsonNode node, String where) {
        JsonNode value = node.path("market");
        if (!value.isString() || !MARKET.matcher(value.asString()).matches()) {
            throw error(where, "market 형식 오류가 있습니다(영문 대문자 2~4자)");
        }
        return value.asString();
    }

    private static String symbol(JsonNode node, String where) {
        JsonNode value = node.path("symbol");
        if (!value.isString() || !SYMBOL.matcher(value.asString()).matches()) {
            throw error(where, "symbol 형식 오류가 있습니다(영문 대문자·숫자, 종류 구분은 . 또는 - 뒤 1~2자)");
        }
        // 종류 구분 표기는 수집기가 저장하는 SEC 표기 "-"로 맞춘다.
        return value.asString().replace('.', '-');
    }

    private static String optionalText(JsonNode node, String field, String where) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (!value.isString()) {
            throw error(where, field + " 은 문자열이어야 합니다");
        }
        String text = value.asString().strip();
        if (text.isEmpty()) {
            return null;
        }
        if (text.length() > TEXT_MAX) {
            throw error(where, field + " 이 " + TEXT_MAX + "자를 넘습니다");
        }
        return text;
    }

    private static BigDecimal weight(JsonNode node, String where) {
        JsonNode value = node.path("weight");
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (!value.isNumber()) {
            throw error(where, "weight 는 숫자여야 합니다");
        }
        BigDecimal weight = value.decimalValue().stripTrailingZeros();
        if (weight.signum() < 0 || weight.compareTo(BigDecimal.valueOf(100)) > 0 || weight.scale() > 4) {
            throw error(where, "weight 는 0 이상 100 이하이고 소수 4자리까지여야 합니다");
        }
        return weight;
    }

    // 순위가 없으면 적힌 순서를 순위로 삼는다.
    private static int rank(JsonNode node, int position, String where) {
        JsonNode value = node.path("rank");
        if (value.isMissingNode() || value.isNull()) {
            return position;
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 1) {
            throw error(where, "rank 는 1 이상의 정수여야 합니다");
        }
        return value.intValue();
    }

    private static IllegalArgumentException error(String where, String kind) {
        return new IllegalArgumentException("보유 종목 파일의 " + where + "에 " + kind);
    }
}
