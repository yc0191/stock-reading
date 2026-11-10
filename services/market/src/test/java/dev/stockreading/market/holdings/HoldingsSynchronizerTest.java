package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import tools.jackson.databind.json.JsonMapper;

/** 파일은 빈 테이블을 처음 채울 때만 쓴다. 가짜 값(TEST 접두)만 쓴다. */
class HoldingsSynchronizerTest {

    @TempDir
    Path dir;

    private HoldingsSynchronizer synchronizer(String file, HoldingsRepository repository) {
        return new HoldingsSynchronizer(
                new ConfiguredHoldings(new HoldingsProperties(file), JsonMapper.builder().build()), repository);
    }

    @Test
    void emptyTableIsFilledFromTheFileOnce() throws Exception {
        Path file = dir.resolve("holdings.json");
        Files.writeString(file, "[{\"kind\":\"ETF\",\"market\":\"KR\",\"symbol\":\"TEST01\"}]");
        HoldingsRepository repository = mock(HoldingsRepository.class);
        when(repository.isEmpty()).thenReturn(true);

        synchronizer(file.toString(), repository).afterSingletonsInstantiated();

        verify(repository).replaceAll(org.mockito.ArgumentMatchers.argThat(plan -> plan.instruments().size() == 1));
    }

    @Test
    void tableWithRowsIsNeverOverwrittenAndTheFileIsNotEvenRead() {
        HoldingsRepository repository = mock(HoldingsRepository.class);
        when(repository.isEmpty()).thenReturn(false);

        // 없는 파일이어도 시작이 실패하지 않는다.
        synchronizer(dir.resolve("no-such-file.json").toString(), repository).afterSingletonsInstantiated();
        synchronizer("", repository).afterSingletonsInstantiated();

        verify(repository, never()).replaceAll(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void emptyTableWithAnUnreadableFileFailsStartup() {
        HoldingsRepository repository = mock(HoldingsRepository.class);
        when(repository.isEmpty()).thenReturn(true);

        assertThatThrownBy(() -> synchronizer(dir.resolve("no-such-file.json").toString(), repository)
                .afterSingletonsInstantiated()).isInstanceOf(IllegalStateException.class);
    }
}
