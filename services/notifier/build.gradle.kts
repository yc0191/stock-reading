plugins {
    id("stock.spring-service")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-mail")

    testImplementation(libs.json.schema.validator)
    testImplementation(libs.greenmail.junit5)
}
