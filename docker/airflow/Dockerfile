# bda/airflow: Airflow 3 + Docker CLI. Tasks run `docker exec bda-edge pulse ...` (the same commands
# as make and the viva), and stage switches run scripts/stage.sh through `docker compose`.
FROM apache/airflow:slim-3.3.2-python3.12
USER root
# Docker's signed apt repo: CLI + compose plugin only (no daemon)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
      > /etc/apt/sources.list.d/docker.list \
 && apt-get update && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*
USER airflow
# Postgres metadata DB: Airflow 3 uses both a sync (psycopg2) and an async (asyncpg) engine
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 asyncpg==0.30.0
