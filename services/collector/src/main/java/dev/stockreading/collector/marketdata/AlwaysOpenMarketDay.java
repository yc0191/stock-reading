package dev.stockreading.collector.marketdata;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** 시세 제공자를 쓰지 않는 모드(가짜 시세)에서는 휴장일을 알 방법이 없어 늘 영업일로 본다. */
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "simulator", matchIfMissing = true)
class AlwaysOpenMarketDay implements MarketDayPort {

    @Override
    public boolean isBusinessDay() {
        return true;
    }
}
