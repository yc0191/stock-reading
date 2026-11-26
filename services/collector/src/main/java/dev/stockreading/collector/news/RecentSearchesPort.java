package dev.stockreading.collector.news;

import java.util.List;
import java.util.Optional;

/**
 * 최근 검색 결과 보관소. 같은 검색어를 짧은 간격에 다시 검색하지 않으려고 둔다(#98).
 * 담는 결과는 검색 제공자가 준 원문(거르기 전)이며, 다시 쓰는 쪽이 그때 시각으로 다시 거른다.
 * 잃어도 되는 캐시라 구현이 실패하면 비어 있는 것처럼 굴어 부르는 쪽이 그냥 검색하게 한다.
 * 검색어는 keyOf 로 맞춘 값을 넘긴다.
 */
interface RecentSearchesPort {

    /**
     * 재검색이 이 검색어의 결과를 다시 쓸지, 직접 검색할지 정한다. 다시 쓸 결과가 있으면 그것을 돌려주고,
     * 아무도 검색 중이 아니면 검색 중 표시를 잡아 준다. 다른 쪽이 검색 중이면 대기 상한까지 그 결과를 기다리며,
     * 그 검색이 실패하면 처음부터 다시 정한다(깨어난 여럿 중 한 곳만 표시를 잡는다). 상한을 넘기면 표시 없이 검색하게 한다.
     */
    Turn take(String key);

    /** 정기 수집용. 기다리지 않고 표시만 시도한다. 이미 다른 쪽이 검색 중이면 false 다. */
    boolean claim(String key);

    /** 검색 결과를 남기고 검색 중 표시를 푼다. 기다리던 쪽은 이 결과를 받는다. */
    void record(String key, List<NewsItem> found);

    /** 검색하지 못했을 때 검색 중 표시를 푼다. 기다리던 쪽은 결과 없이 깨어나 차례를 다시 정한다. */
    void release(String key);

    /** 앞뒤 공백을 지우고 연속 공백을 하나로 줄인다. 대소문자는 그대로 구분한다. */
    static String keyOf(String query) {
        return String.join(" ", query.strip().split("\\s+"));
    }

    /** take 의 결과. reused 가 있으면 다시 쓰고, 없으면 검색한다. owner 이면 검색 뒤 record 나 release 로 표시를 푼다. */
    record Turn(Optional<List<NewsItem>> reused, boolean owner) {

        static Turn reuse(List<NewsItem> items) {
            return new Turn(Optional.of(items), false);
        }

        static Turn search(boolean owner) {
            return new Turn(Optional.empty(), owner);
        }
    }
}
