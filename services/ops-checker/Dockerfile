# check=skip=FromPlatformFlagConstDisallowed
# 서버가 x86(linux/amd64)이라 실행 이미지는 빌드하는 기계와 무관하게 amd64 로 고정한다(경고를 일부러 끈다).
# 빌드 컨텍스트는 저장소 루트다: docker build -f services/ops-checker/Dockerfile .
FROM --platform=linux/amd64 python:3.13-slim AS build
WORKDIR /build
RUN python -m venv /opt/venv
COPY services/ops-checker/requirements.txt .
# 컴파일 도구 없이 설치하려고 미리 만든 바퀴(wheel)만 받는다. 시험 도구(pytest)도 함께 들어가나 실행에는 쓰지 않는다.
RUN /opt/venv/bin/pip install --no-cache-dir --only-binary=:all: -r requirements.txt

FROM --platform=linux/amd64 python:3.13-slim
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=build /opt/venv /opt/venv
COPY services/ops-checker/src /app/src
# 기준 파일은 이미지에 실려 배포된다. 이 이미지 버전(DEPLOY_VERSION)이 곧 그날 판정에 쓴 기준의 버전이다.
COPY ops-checks /app/ops-checks
USER app
WORKDIR /app
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1 OPS_CRITERIA_DIR=/app/ops-checks PATH=/opt/venv/bin:$PATH
ENTRYPOINT ["python", "-m", "ops_checker.main"]
