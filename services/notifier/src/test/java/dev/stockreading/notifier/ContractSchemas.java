package dev.stockreading.notifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;

public final class ContractSchemas {

    private static final Path DIR = Path.of("../../contracts/events");

    // 형식 검증(date-time, uuid)은 기본으로 꺼져 있어서, 켜야 잘못된 시각을 잡는다.
    private static final SchemaRegistry REGISTRY = SchemaRegistry.withDefaultDialect(
            SpecificationVersion.DRAFT_2020_12,
            builder -> builder.schemaRegistryConfig(
                    SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build()));

    private ContractSchemas() {
    }

    public static List<Error> validate(String schemaFile, String json) throws IOException {
        String schemaText = Files.readString(DIR.resolve(schemaFile));
        return REGISTRY.getSchema(schemaText).validate(json, InputFormat.JSON);
    }

    public static String sample(String sampleFile) throws IOException {
        return sample("v1", sampleFile);
    }

    public static String sample(String version, String sampleFile) throws IOException {
        return Files.readString(DIR.resolve("samples").resolve(version).resolve(sampleFile));
    }
}
