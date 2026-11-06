package dev.stockreading.market.holdings;

import org.springframework.boot.context.properties.ConfigurationProperties;

// file 은 보유 종목 JSON 파일의 경로이고, 파일 내용은 저장소 밖에만 둔다. 비어 있으면 보유 종목이 없는 것으로 본다.
// usByEtf 는 수집기가 읽는 "ETF코드=티커,티커;ETF코드2=티커" 값이며, 여기서는 파일과 어긋나지 않는지 비교하는 데만 쓴다.
@ConfigurationProperties("market.holdings")
public record HoldingsProperties(String usByEtf, String file) {

    // 설정 값이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "HoldingsProperties[usByEtf=***, file=***]";
    }
}
