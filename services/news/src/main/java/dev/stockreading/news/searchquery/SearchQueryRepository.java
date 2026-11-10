package dev.stockreading.news.searchquery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import dev.stockreading.news.event.SearchQueriesPayload;

/** 검색어(search_query)와 그 검색어가 붙일 종목(search_query_symbol) 테이블을 읽고, 비어 있을 때 처음 채운다. */
@Repository
class SearchQueryRepository {

    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;

    SearchQueryRepository(JdbcClient jdbc, TransactionTemplate transaction) {
        this.jdbc = jdbc;
        this.transaction = transaction;
    }

    boolean isEmpty() {
        return jdbc.sql("SELECT NOT EXISTS (SELECT 1 FROM search_query)").query(Boolean.class).single();
    }

    /**
     * 두 테이블의 전체 목록을 한 번에 읽는다. 읽는 중에 다른 쪽이 고치면 검색어와 종목이 어긋나게 읽히므로
     * 한 트랜잭션(REPEATABLE READ)에서 두 번 읽는다. 종목이 하나도 없는 검색어는 태그를 붙일 수 없어 목록에서 뺀다.
     */
    SearchQuerySnapshot readSnapshot() {
        TransactionTemplate readOnly = new TransactionTemplate(transaction.getTransactionManager());
        readOnly.setReadOnly(true);
        readOnly.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        return readOnly.execute(status -> {
            Map<Long, String> queryById = new LinkedHashMap<>();
            jdbc.sql("SELECT id, query FROM search_query ORDER BY id")
                    .query((rs, rowNum) -> queryById.put(rs.getLong("id"), rs.getString("query"))).list();
            Map<Long, List<String>> symbolsById = new LinkedHashMap<>();
            jdbc.sql("SELECT query_id, symbol FROM search_query_symbol ORDER BY query_id, symbol")
                    .query((rs, rowNum) -> symbolsById.computeIfAbsent(rs.getLong("query_id"), k -> new ArrayList<>())
                            .add(rs.getString("symbol")))
                    .list();
            List<SearchQueriesPayload.Query> queries = new ArrayList<>();
            queryById.forEach((id, query) -> {
                List<String> symbols = symbolsById.get(id);
                if (symbols != null && !symbols.isEmpty()) {
                    queries.add(new SearchQueriesPayload.Query(query, symbols));
                }
            });
            return new SearchQuerySnapshot(queries);
        });
    }

    /** 빈 테이블을 처음 채운다. 한 트랜잭션이라 중간에 실패하면 아무것도 남지 않는다. */
    void insertAll(List<SearchQueriesPayload.Query> queries) {
        transaction.executeWithoutResult(status -> {
            for (SearchQueriesPayload.Query q : queries) {
                long id = jdbc.sql("INSERT INTO search_query (query) VALUES (:query) RETURNING id")
                        .param("query", q.query()).query(Long.class).single();
                for (String symbol : q.symbols()) {
                    jdbc.sql("INSERT INTO search_query_symbol (query_id, symbol) VALUES (:id, :symbol)")
                            .param("id", id).param("symbol", symbol).update();
                }
            }
        });
    }

    /**
     * 종목 이름으로 만든 검색어를 넣고 새로 넣은 검색어 수를 돌려준다. 이미 검색어가 붙은 종목은 빼고, 같은 글자의 검색어가
     * 이미 있으면 그 행을 건드리지 않고 건너뛴다. 한 트랜잭션이라 중간에 실패하면 아무것도 남지 않는다.
     */
    int insertGenerated(Map<String, ? extends Collection<String>> symbolsByName) {
        Integer created = transaction.execute(status -> {
            Set<String> tagged = new HashSet<>(jdbc.sql("SELECT DISTINCT symbol FROM search_query_symbol")
                    .query(String.class).list());
            int count = 0;
            for (Map.Entry<String, ? extends Collection<String>> entry : symbolsByName.entrySet()) {
                List<String> symbols = entry.getValue().stream().filter(s -> !tagged.contains(s)).sorted().toList();
                if (symbols.isEmpty()) {
                    continue;
                }
                Long id = jdbc.sql("INSERT INTO search_query (query) VALUES (:query) ON CONFLICT (query) DO NOTHING RETURNING id")
                        .param("query", entry.getKey()).query(Long.class).optional().orElse(null);
                if (id == null) {
                    continue;
                }
                for (String symbol : symbols) {
                    jdbc.sql("INSERT INTO search_query_symbol (query_id, symbol) VALUES (:id, :symbol)")
                            .param("id", id).param("symbol", symbol).update();
                }
                count++;
            }
            return count;
        });
        return created == null ? 0 : created;
    }
}
