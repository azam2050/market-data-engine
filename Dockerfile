# Runs the live engine 24/5. Kept deliberately small: this container has to
# survive months of continuous operation, so it carries no build tooling and no
# shell conveniences it does not need.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=UTC

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir -e . && \
    useradd --create-home --uid 10001 engine && \
    mkdir -p /app/var && chown -R engine:engine /app

USER engine

# journal and session state must outlive the container: a restart with an open
# position depends on this directory persisting. Railway's builder rejects the
# Docker VOLUME instruction outright — persistence is configured as a Railway
# Volume mounted at /app/var from the dashboard instead (see docs/DEPLOY.md).

# shadow mode by default — the engine should never claim a track record it has
# not earned, and switching that on must be a deliberate act
CMD ["qqq", "live", "--shadow"]
