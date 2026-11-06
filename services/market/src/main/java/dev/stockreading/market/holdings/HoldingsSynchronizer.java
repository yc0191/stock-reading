package dev.stockreading.market.holdings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

/**
 * 시작할 때 보유 종목 파일의 내용으로 테이블을 똑같이 맞춘다(배포 전까지의 임시 경로).
 * 카프카 리스너가 시작하기 전에 끝나도록 모든 빈이 만들어진 직후에 돈다. 파일에 없는 행은 지워진다.
 */
@Component
class HoldingsSynchronizer implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(HoldingsSynchronizer.class);

    private final ConfiguredHoldings configured;
    private final HoldingsRepository repository;

    HoldingsSynchronizer(ConfiguredHoldings configured, HoldingsRepository repository) {
        this.configured = configured;
        this.repository = repository;
    }

    @Override
    public void afterSingletonsInstantiated() {
        repository.replaceAll(configured.plan());
        log.info("보유 종목 테이블을 파일과 맞췄습니다: 종목 {}개, 구성 관계 {}개",
                configured.plan().instruments().size(), configured.plan().memberships().size());
    }
}
