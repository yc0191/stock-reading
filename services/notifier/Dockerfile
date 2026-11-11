# check=skip=FromPlatformFlagConstDisallowed
# 서버가 x86(linux/amd64)이라 실행 이미지는 빌드하는 기계와 무관하게 amd64 로 고정한다(경고를 일부러 끈다).
# 빌드 컨텍스트는 저장소 루트다: docker build -f services/notifier/Dockerfile .
# jar 는 CPU 종류와 무관하므로 빌드 단계는 빌드하는 기계의 CPU 로 돌려(에뮬레이션을 피함), 실행 이미지만 서버(Lightsail, x86)에 맞춘다.
FROM --platform=$BUILDPLATFORM eclipse-temurin:21-jdk AS build
WORKDIR /src
# Gradle 이 toolchain 을 환경변수에서 찾는다(gradle.properties 의 org.gradle.java.installations.fromEnv).
ENV JAVA_HOME_21=/opt/java/openjdk
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
COPY build-logic build-logic
# settings.gradle.kts 가 서비스 모듈 4개를 모두 가리키므로 빌드 파일은 모두 둔다. 소스는 이 서비스 것만 둔다.
COPY services/collector/build.gradle.kts services/collector/build.gradle.kts
COPY services/market/build.gradle.kts services/market/build.gradle.kts
COPY services/news/build.gradle.kts services/news/build.gradle.kts
COPY services/notifier/build.gradle.kts services/notifier/build.gradle.kts
COPY services/notifier/src services/notifier/src
# bootJar 와 함께 만들어지는 -plain.jar 는 실행할 수 없으므로 뺀다.
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew :services:notifier:bootJar -x test --no-daemon --console=plain \
    && cp "$(ls services/notifier/build/libs/*.jar | grep -v -- '-plain.jar' | head -n 1)" /app.jar

FROM --platform=linux/amd64 eclipse-temurin:21-jre
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=build /app.jar /app/notifier.jar
USER app
WORKDIR /app
# 힙 상한 등 JVM 옵션은 운영 Compose 의 JAVA_TOOL_OPTIONS 로 준다.
ENTRYPOINT ["java", "-jar", "/app/notifier.jar"]
