plugins {
    id("stock.spring-service")
}

dependencies {
    testImplementation(libs.json.schema.validator)
}
