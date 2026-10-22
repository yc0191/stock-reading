package dev.stockreading.collector.disclosure;

import java.time.LocalDate;
import java.util.List;

public interface DisclosurePort {

    /** companyCode 가 비어 있으면 전체 회사가 대상이다. */
    List<Disclosure> list(String companyCode, LocalDate from, LocalDate to);
}
