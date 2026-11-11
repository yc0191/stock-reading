# check=skip=FromPlatformFlagConstDisallowed
# 서버가 x86(linux/amd64)이라 실행 이미지는 빌드하는 기계와 무관하게 amd64 로 고정한다(경고를 일부러 끈다).
# 빌드 컨텍스트는 저장소 루트다: docker build -f services/briefing/Dockerfile .
# 의존성 설치(venv)와 글꼴 내려받기는 실행 이미지와 같은 x86 에서 해야 하므로 단계 모두 linux/amd64 로 고정한다.
FROM --platform=linux/amd64 python:3.13-slim AS build
WORKDIR /build
RUN python -m venv /opt/venv
COPY services/briefing/requirements.txt .
# 컴파일 도구 없이 설치하려고 미리 만든 바퀴(wheel)만 받는다.
RUN /opt/venv/bin/pip install --no-cache-dir --only-binary=:all: -r requirements.txt
# 글꼴은 git 에 없고 버전·SHA-256 이 고정된 주소에서 받는다(scripts/install_font.py 가 SHA-256 이 다르면 실패).
COPY scripts/install_font.py scripts/install_font.py
RUN python scripts/install_font.py

FROM --platform=linux/amd64 python:3.13-slim
# 브리핑은 DB 주소를 localhost:5432 로 고정해 읽는다(코드). 데이터 서버에 닿게 컨테이너 안에서 localhost:5432 를 DB_HOST:DB_PORT 로 넘긴다.
RUN apt-get update && apt-get install -y --no-install-recommends socat \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home app
COPY --from=build /opt/venv /opt/venv
COPY --from=build /build/services/briefing/fonts /app/fonts
COPY services/briefing/src /app/src
COPY services/briefing/docker-entrypoint.sh /app/docker-entrypoint.sh
USER app
WORKDIR /app
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1 MPLCONFIGDIR=/tmp/matplotlib BRIEFING_FONT_DIR=/app/fonts PATH=/opt/venv/bin:$PATH
ENTRYPOINT ["/app/docker-entrypoint.sh"]
