# syntax=docker/dockerfile:1.7
FROM ghcr.io/astral-sh/uv:0.8.13 AS uv

FROM python:3.12.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LANGGRAPH_STRICT_MSGPACK=true \
    UV_NATIVE_TLS=true \
    PYTHONPATH="/app/src" \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --system --gid 10001 axms \
    && useradd --system --uid 10001 --gid axms --home-dir /app --shell /usr/sbin/nologin axms

WORKDIR /app
COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md ./
ARG UV_INSECURE_HOST=""
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src

USER 10001:10001
EXPOSE 8090
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 \
    CMD ["python", "-m", "axms_coding_orchestrator.healthcheck"]

CMD ["python", "-m", "axms_coding_orchestrator.service"]
