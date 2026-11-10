package dev.stockreading.market.holdings;

import org.springframework.boot.context.properties.ConfigurationProperties;

// file 은 보유 종목 JSON 파일의 경로이고, 파일 내용은 저장소 밖에만 둔다. 종목 테이블이 비어 있을 때 처음 채우는 데만 쓴다.
// 테이블이 바뀌었는지 확인하는 간격(market.holdings.check-interval)은 HoldingsChangeWatcher 가 설정에서 바로 읽는다.
@ConfigurationProperties("market.holdings")
public record HoldingsProperties(String file) {

    // 설정 값이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "HoldingsProperties[file=***]";
    }
}
