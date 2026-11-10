package dev.stockreading.market.holdings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

/**
 * 시작할 때 종목 테이블이 비어 있으면 보유 종목 파일로 처음 한 번 채운다(배포 전까지의 임시 경로).
 * 테이블에 행이 있으면 파일을 읽지도 않는다. 그 뒤의 고침은 테이블을 직접 고치고, 이미 있는 행을 파일이 덮어쓰거나
 * 지우면 직접 고친 내용이 재시작 때 사라지기 때문이다. 카프카 리스너가 시작하기 전에 끝나도록 모든 빈이 만들어진 직후에 돈다.
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
        if (!repository.isEmpty()) {
            log.info("보유 종목 테이블에 이미 행이 있어 파일로 채우지 않습니다");
            return;
        }
        HoldingsPlan plan = configured.plan();
        repository.replaceAll(plan);
        log.info("보유 종목 테이블이 비어 있어 파일로 처음 채웠습니다: 종목 {}개, 구성 관계 {}개",
                plan.instruments().size(), plan.memberships().size());
    }
}
