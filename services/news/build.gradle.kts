plugins {
    id("stock.spring-service")
}

dependencies {
    // 브리핑이 부르는 내부 조회 API. 수집기 등 다른 서비스에는 웹 서버를 넣지 않으므로 공통 플러그인이 아닌 여기에 둔다.
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation(libs.json.schema.validator)
}
