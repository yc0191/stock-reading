package dev.stockreading.market.holdings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

/**
 * 저장소 밖 보유 종목 파일(market.holdings.file)을 읽고 검증해 목표 상태(plan)로 돌려준다.
 * 종목 테이블이 비어 있을 때 처음 채우는 데만 쓰므로 필요할 때(plan 호출)만 읽는다. 테이블에 행이 있으면 파일이
 * 없거나 비어 있어도 시작에 영향이 없어야 해서 생성자에서는 읽지 않는다.
 */
@Component
class ConfiguredHoldings {

    private static final Logger log = LoggerFactory.getLogger(ConfiguredHoldings.class);

    private final HoldingsProperties properties;
    private final ObjectMapper mapper;

    ConfiguredHoldings(HoldingsProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
    }

    HoldingsPlan plan() {
        HoldingsPlan plan = HoldingsFileParser.parse(read(properties.file()), mapper);
        // 코드와 티커는 보유 종목이 드러나므로 개수만 남긴다.
        log.info("보유 종목 파일을 읽었습니다: 종목 {}개, 구성 관계 {}개", plan.instruments().size(), plan.memberships().size());
        return plan;
    }

    // 파일이 없거나 못 읽는 것도 실패다. 경로는 메시지에 넣지 않는다.
    private static String read(String file) {
        if (file == null || file.isBlank()) {
            return "";
        }
        try {
            return Files.readString(Path.of(file.strip()), StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("보유 종목 파일을 읽을 수 없습니다");
        }
    }
}
