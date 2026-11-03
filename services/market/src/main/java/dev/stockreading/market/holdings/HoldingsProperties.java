package dev.stockreading.market.holdings;

import org.springframework.boot.context.properties.ConfigurationProperties;

// usByEtf 는 "ETF코드=티커,티커;ETF코드2=티커" 형식이며 보유 ETF와 상위 종목이 드러나므로 저장소에 두지 않는다.
// 형식 검증은 ConfiguredHoldings 가 시작할 때 한다.
@ConfigurationProperties("market.holdings")
public record HoldingsProperties(String usByEtf) {

    // 설정 값이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "HoldingsProperties[usByEtf=***]";
    }
}
