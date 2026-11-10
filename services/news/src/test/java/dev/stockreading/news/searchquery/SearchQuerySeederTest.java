package dev.stockreading.news.searchquery;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 파일은 빈 테이블을 처음 채울 때만 쓴다. 가짜 값(TEST 접두 종목코드)만 쓴다. */
class SearchQuerySeederTest {

    @TempDir
    Path dir;

    private static SearchQuerySeeder seeder(String file, SearchQueryRepository repository) {
        return new SearchQuerySeeder(new SearchQueryProperties(file), repository);
    }

    @Test
    void emptyTableIsFilledFromTheFileOnce() throws Exception {
        Path file = dir.resolve("queries.txt");
        Files.writeString(file, "가짜 검색어=TEST01,TEST02;가짜 시황=*");
        SearchQueryRepository repository = mock(SearchQueryRepository.class);
        when(repository.isEmpty()).thenReturn(true);

        seeder(file.toString(), repository).afterSingletonsInstantiated();

        verify(repository).insertAll(argThat(queries -> queries.size() == 2));
    }

    @Test
    void tableWithRowsIsNeverOverwrittenAndTheFileIsNotEvenRead() {
        SearchQueryRepository repository = mock(SearchQueryRepository.class);
        when(repository.isEmpty()).thenReturn(false);

        // 없는 파일이어도 시작이 실패하지 않는다.
        seeder(dir.resolve("no-such-file.txt").toString(), repository).afterSingletonsInstantiated();
        seeder("", repository).afterSingletonsInstantiated();

        verify(repository, never()).insertAll(any());
    }

    @Test
    void emptyTableWithNoFileOrABlankFileIsANormalStart() throws Exception {
        Path blank = dir.resolve("blank.txt");
        Files.writeString(blank, "  \n");
        SearchQueryRepository repository = mock(SearchQueryRepository.class);
        when(repository.isEmpty()).thenReturn(true);

        seeder(null, repository).afterSingletonsInstantiated();
        seeder(blank.toString(), repository).afterSingletonsInstantiated();
    }

    @Test
    void emptyTableWithAnUnreadableOrMalformedFileFailsStartupWithoutThePathOrValues() throws Exception {
        Path bad = dir.resolve("bad.txt");
        Files.writeString(bad, "가짜=test01");
        SearchQueryRepository repository = mock(SearchQueryRepository.class);
        when(repository.isEmpty()).thenReturn(true);

        assertThatThrownBy(() -> seeder(dir.resolve("no-such-file.txt").toString(), repository)
                .afterSingletonsInstantiated())
                .isInstanceOf(IllegalStateException.class).hasMessageNotContaining("no-such-file");
        assertThatThrownBy(() -> seeder(bad.toString(), repository).afterSingletonsInstantiated())
                .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("test01");
        verify(repository, never()).insertAll(any());
    }
}
