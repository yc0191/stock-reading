# check=skip=FromPlatformFlagConstDisallowed
# DB 표 만들기(마이그레이션) 이미지. 빌드 컨텍스트는 저장소 루트다: docker build -f infra/deploy/migrate/Dockerfile .
# 서비스 이미지와 같은 버전 태그로 올려, 서버의 저장소 사본과 상관없이 배포한 버전의 SQL 파일이 그대로 적용되게 한다.
# personal 계정·권한 스크립트(init.sql)가 psql 로 돌아 psql 이 들어 있는 postgres 이미지를 바탕으로 하고, Flyway 는 공식 이미지에서 복사한다.
# 빌드 중에 패키지 저장소(apt)를 부르지 않는다. CI 러너에서 Ubuntu 저장소 이름을 찾지 못해 빌드가 거듭 실패했다.
FROM --platform=linux/amd64 flyway/flyway:12.4.0 AS flyway

FROM --platform=linux/amd64 postgres:17.6-bookworm
COPY --from=flyway /flyway /flyway
COPY --from=flyway /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/flyway:/opt/java/openjdk/bin:$PATH
COPY services/market/src/main/resources/db/migration /sql/market
COPY infra/db/personal/init.sql /sql/personal-init.sql
COPY infra/db/personal/migration /sql/personal
COPY services/briefing/db/migration /sql/briefing
COPY infra/deploy/migrate/migrate.sh /usr/local/bin/migrate.sh
RUN chmod +x /usr/local/bin/migrate.sh
# 비밀번호를 받는 컨테이너이므로 root 로 돌리지 않는다. 베이스 이미지에는 일반 사용자가 없어 만든다.
RUN useradd --system --uid 10001 --home-dir /tmp --no-create-home flyway
USER flyway
ENTRYPOINT ["/usr/local/bin/migrate.sh"]
