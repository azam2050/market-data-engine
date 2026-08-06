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
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# gosu drops root privileges after the entrypoint fixes volume ownership —
# needed because Railway mounts the persistent volume as root at container
# start, after this build-time chown has already run.
RUN pip install --no-cache-dir -e . && \
    apt-get update && apt-get install -y --no-install-recommends gosu && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --create-home --uid 10001 engine && \
    mkdir -p /app/var && chown -R engine:engine /app && \
    chmod +x /usr/local/bin/docker-entrypoint.sh

# documentation only — Railway routes to whatever $PORT the process actually
# listens on; the dashboard is off by default until ADMIN_USERNAME/PASSWORD
# are set (see qqq_alpha/config.py)
EXPOSE 8080

# journal and session state must outlive the container: a restart with an open
# position depends on this directory persisting. Railway's builder rejects the
# Docker VOLUME instruction outright — persistence is configured as a Railway
# Volume mounted at /app/var from the dashboard instead (see docs/DEPLOY.md).
# The container starts as root so the entrypoint can chown that mount before
# dropping to the unprivileged engine user (see docker-entrypoint.sh).
ENTRYPOINT ["docker-entrypoint.sh"]

# shadow mode by default — the engine should never claim a track record it has
# not earned, and switching that on must be a deliberate act
CMD ["qqq", "live", "--shadow"]
