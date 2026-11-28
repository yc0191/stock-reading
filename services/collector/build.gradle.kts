plugins {
    id("stock.spring-service")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    runtimeOnly("org.postgresql:postgresql")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")

    testImplementation(libs.json.schema.validator)
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
}
