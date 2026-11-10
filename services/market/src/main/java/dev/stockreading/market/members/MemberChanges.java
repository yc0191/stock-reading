package dev.stockreading.market.members;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 상위 구성이 바뀌기 전후의 차이. 종목은 removed·added·moved 중 한 곳에만 나오고, 상위 구성 밖으로 나가면 removed에만 나온다.
 * 순위가 같은 종목은 담지 않고, 비중은 비교하지 않는다(날마다 달라져서 구성이 바뀐 이유가 아니다).
 *
 * @param since 바뀌기 전 구성을 마지막으로 확인한 날
 * @param asOf 바뀐 구성을 처음 받은 날
 */
public record MemberChanges(LocalDate since, LocalDate asOf, List<Member> removed, List<Member> added,
        List<Moved> moved) {

    public record Moved(String market, String symbol, String name, int rankBefore, int rankAfter) {
    }

    /** @return 차이가 없으면 빈 값(구성이 바뀌었다가 되돌아온 경우 포함) */
    static Optional<MemberChanges> between(List<Member> before, List<Member> after, LocalDate since, LocalDate asOf) {
        Map<String, Member> beforeByKey = byKey(before);
        Map<String, Member> afterByKey = byKey(after);
        List<Member> removed = new ArrayList<>();
        List<Member> added = new ArrayList<>();
        List<Moved> moved = new ArrayList<>();
        for (Member b : before) {
            Member a = afterByKey.get(key(b));
            if (a == null) {
                removed.add(b);
            } else if (a.rank() != b.rank()) {
                moved.add(new Moved(a.market(), a.symbol(), a.name() != null ? a.name() : b.name(), b.rank(),
                        a.rank()));
            }
        }
        for (Member a : after) {
            if (!beforeByKey.containsKey(key(a))) {
                added.add(a);
            }
        }
        if (removed.isEmpty() && added.isEmpty() && moved.isEmpty()) {
            return Optional.empty();
        }
        removed.sort(Comparator.comparingInt(Member::rank));
        added.sort(Comparator.comparingInt(Member::rank));
        moved.sort(Comparator.comparingInt(Moved::rankAfter));
        return Optional.of(new MemberChanges(since, asOf, removed, added, moved));
    }

    static String key(Member m) {
        return m.market() + ":" + m.symbol();
    }

    private static Map<String, Member> byKey(List<Member> members) {
        Map<String, Member> map = new HashMap<>();
        members.forEach(m -> map.put(key(m), m));
        return map;
    }
}
