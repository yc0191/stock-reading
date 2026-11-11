plugins {
    id("stock.spring-service")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-mail")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation(libs.json.schema.validator)
    testImplementation(libs.greenmail.junit5)
}
