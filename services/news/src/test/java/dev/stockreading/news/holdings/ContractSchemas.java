package dev.stockreading.news.holdings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class ContractSchemas {

    private static final Path DIR = Path.of("../../contracts/events");

    private ContractSchemas() {
    }

    static String sample(String sampleFile) throws IOException {
        return Files.readString(DIR.resolve("samples/v1").resolve(sampleFile));
    }
}
